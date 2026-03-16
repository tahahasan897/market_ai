"""
Telegram Command Center for the Market AI Assistant.

Commands:
    /summary    — Current regime, severity, flags, snapshot, AI note
    /suggestion — Regime-aligned trade ideas and directional bias
    /open       — Open a position ticket  (e.g. /open ES long 2 6635)
    /positions  — List all open positions
    /close      — Close a position by ID  (e.g. /close 3)
    /help       — Show available commands

Runs as a long-polling listener alongside run_monitor.py.
"""

import os
import json
import time
import html
import requests
import traceback
import psycopg2
import anthropic
from decimal import Decimal
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

load_dotenv()

# ─── Config ──────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

EASTERN = ZoneInfo("America/New_York")

POLL_TIMEOUT = 30  # seconds for long-polling
POLL_INTERVAL = 1  # seconds between poll cycles on error

# ─── Helpers (shared patterns from live_detector / alerts) ───────────────────

def esc(text) -> str:
    if text is None:
        return ""
    return html.escape(str(text))


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def send_telegram_message(chat_id: str, message: str):
    """Send an HTML-formatted message to a specific chat."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()


def query_latest_snapshot():
    """Fetch latest prices from InfluxDB (same logic as live_detector)."""
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -7d)
      |> filter(fn: (r) => r["_measurement"] == "market_prices")
      |> filter(fn: (r) => r["_field"] == "close")
      |> filter(fn: (r) =>
          r["instrument"] == "ES" or
          r["instrument"] == "NQ" or
          r["instrument"] == "RTY" or
          r["instrument"] == "YM" or
          r["instrument"] == "VIX" or
          r["instrument"] == "DXY" or
          r["instrument"] == "TNX" or
          r["instrument"] == "GC" or
          r["instrument"] == "CL"
      )
      |> last()
    '''
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    tables = query_api.query(query)
    client.close()

    snapshot = {}
    for table in tables:
        for record in table.records:
            instrument = record.values.get("instrument")
            snapshot[instrument] = {
                "price": float(record.get_value()),
                "time": record.get_time(),
            }
    return snapshot


def snapshot_to_prices(snapshot):
    return {inst: payload["price"] for inst, payload in snapshot.items()}


def detect_market_regime(prices):
    """Same regime logic as live_detector.py."""
    flags = []

    vix = prices.get("VIX")
    dxy = prices.get("DXY")
    tnx = prices.get("TNX")
    gc = prices.get("GC")
    cl = prices.get("CL")
    nq = prices.get("NQ")
    es = prices.get("ES")

    if vix is not None and vix > 22:
        flags.append("high_vix")
    if dxy is not None and dxy > 98.5:
        flags.append("strong_dollar")
    if tnx is not None and tnx > 4.10:
        flags.append("high_yields")
    if gc is not None and gc > 5000:
        flags.append("strong_gold")
    if cl is not None and cl > 80:
        flags.append("firm_oil")
    if nq is not None and nq < 25200:
        flags.append("nq_soft")
    if es is not None and es < 6850:
        flags.append("es_soft")

    regime = "neutral"
    if {"high_vix", "strong_dollar", "high_yields"}.issubset(set(flags)):
        regime = "risk_off_macro_pressure"

    return regime, flags


def compute_severity_score(flags):
    weights = {
        "high_vix": 2, "strong_dollar": 1, "high_yields": 2,
        "strong_gold": 1, "firm_oil": 1, "nq_soft": 1, "es_soft": 1,
    }
    return sum(weights.get(f, 0) for f in flags)


def severity_label_from_score(score):
    if score >= 7:
        return "extreme"
    if score >= 5:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def get_last_meaningful_event():
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, regime, flags, claude_summary, local_summary,
               escalation_used, severity_score, severity_label, status, created_at
        FROM live_market_events
        WHERE status IN ('NEW_REGIME', 'REGIME_WORSENING', 'REGIME_EASING', 'ONGOING_REGIME')
        ORDER BY created_at DESC
        LIMIT 1;
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        return None
    return {
        "id": row[0], "regime": row[1], "flags": row[2],
        "claude_summary": row[3], "local_summary": row[4],
        "escalation_used": row[5], "severity_score": row[6],
        "severity_label": row[7], "status": row[8], "created_at": row[9],
    }


# ─── /summary ────────────────────────────────────────────────────────────────

def handle_summary(chat_id: str):
    snapshot = query_latest_snapshot()
    if not snapshot:
        send_telegram_message(chat_id, "No market data available in InfluxDB.")
        return

    prices = snapshot_to_prices(snapshot)
    regime, flags = detect_market_regime(prices)
    severity_score = compute_severity_score(flags)
    severity_label = severity_label_from_score(severity_score)

    last_event = get_last_meaningful_event()
    last_status = last_event["status"] if last_event else "N/A"
    last_time = last_event["created_at"].strftime("%Y-%m-%d %I:%M %p") if last_event else "N/A"

    ai_note = None
    if last_event:
        ai_note = last_event.get("claude_summary") or last_event.get("local_summary")

    lines = [
        "📊 <b>MARKET SUMMARY</b>",
        f"<b>Time:</b> {esc(datetime.now(EASTERN).strftime('%Y-%m-%d %I:%M %p ET'))}",
        "",
        "<b>── Regime ──</b>",
        f"<b>Current:</b> {esc(regime)}",
        f"<b>Severity:</b> {esc(severity_label)} ({esc(severity_score)})",
        f"<b>Flags:</b> {esc(', '.join(sorted(flags)) if flags else 'none')}",
        f"<b>Last Event:</b> {esc(last_status)} at {esc(last_time)}",
        "",
        "<b>── Snapshot ──</b>",
        f"ES: {esc(prices.get('ES'))}  |  NQ: {esc(prices.get('NQ'))}",
        f"RTY: {esc(prices.get('RTY'))}  |  YM: {esc(prices.get('YM'))}",
        f"VIX: {esc(prices.get('VIX'))}  |  DXY: {esc(prices.get('DXY'))}",
        f"TNX: {esc(prices.get('TNX'))}  |  GC: {esc(prices.get('GC'))}",
        f"CL: {esc(prices.get('CL'))}",
    ]

    if ai_note:
        lines.extend([
            "",
            "<b>── AI Note ──</b>",
            esc(ai_note[:1200]),
        ])

    send_telegram_message(chat_id, "\n".join(lines))


# ─── /suggestion ──────────────────────────────────────────────────────────────

SUGGESTION_PROMPT = """You are a cross-asset market strategist advising a swing trader who holds positions for days to weeks.

Given the current market snapshot and detected regime, produce a concise trade suggestion note.

Rules:
- Do NOT issue direct buy or sell orders — these are analytical suggestions only
- The trader does NOT use hard stop-loss orders on the exchange (due to stop hunting concerns)
- Instead, provide soft stop levels as "risk alert" prices where the trader should reassess
- Use only the data provided — do not invent prices or levels not derivable from the snapshot
- Base entry and stop levels on the current prices shown — use round numbers or nearby support/resistance
- Keep it concise (under 600 words)

Format your response exactly like this:

REGIME BIAS: [your one-line regime interpretation]

ALIGNED ASSETS:
- [asset]: [why it aligns with regime]

CONFLICTING ASSETS:
- [asset]: [why it conflicts]

WHAT WOULD INVALIDATE THIS THESIS:
- [condition 1]
- [condition 2]

SWING TRADE CONSIDERATIONS:
For each idea (1-2 max), include:
- Asset and directional bias (long/short)
- Suggested entry zone or limit price
- Soft stop (risk alert level — where to reassess, NOT a hard exchange stop)
- Reasoning tied to the current regime and cross-asset context

Example format for each idea:
  IDEA: ES long bias
  ENTRY: limit near [price] or on pullback to [zone]
  SOFT STOP: reassess below [price] (reason)
  RATIONALE: [why this aligns with regime]

Market snapshot:
ES: {es}
NQ: {nq}
RTY: {rty}
YM: {ym}
VIX: {vix}
DXY: {dxy}
TNX: {tnx}
GC: {gc}
CL: {cl}

Detected regime: {regime}
Flags: {flags}
"""


def handle_suggestion(chat_id: str):
    snapshot = query_latest_snapshot()
    if not snapshot:
        send_telegram_message(chat_id, "No market data available.")
        return

    prices = snapshot_to_prices(snapshot)
    regime, flags = detect_market_regime(prices)

    prompt = SUGGESTION_PROMPT.format(
        es=prices.get("ES"), nq=prices.get("NQ"),
        rty=prices.get("RTY"), ym=prices.get("YM"),
        vix=prices.get("VIX"), dxy=prices.get("DXY"),
        tnx=prices.get("TNX"), gc=prices.get("GC"),
        cl=prices.get("CL"),
        regime=regime,
        flags=", ".join(sorted(flags)) if flags else "none",
    )

    send_telegram_message(chat_id, "🔄 <b>Generating suggestion…</b> (calling Claude)")

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=90.0)
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        parts = []
        for block in message.content:
            if getattr(block, "type", None) == "text":
                parts.append(block.text)
        suggestion_text = "\n".join(parts).strip()
    except Exception as e:
        send_telegram_message(chat_id, f"Claude suggestion failed: {esc(str(e))}")
        return

    severity_score = compute_severity_score(flags)
    severity_label = severity_label_from_score(severity_score)

    lines = [
        "💡 <b>TRADE SUGGESTION</b>",
        f"<b>Regime:</b> {esc(regime)} | <b>Severity:</b> {esc(severity_label)} ({esc(severity_score)})",
        "",
        esc(suggestion_text[:2500]),
        "",
        "<i>⚠️ Analysis only — not a trade order. Stop levels are soft risk alerts, not exchange orders.</i>",
    ]

    send_telegram_message(chat_id, "\n".join(lines))


# ─── /open ────────────────────────────────────────────────────────────────────

def handle_open(chat_id: str, args: list):
    """
    Usage: /open SYMBOL SIDE SIZE ENTRY_PRICE [notes=...]
    Example: /open ES long 2 6635
    Example: /open ES long 2 6635 thesis=risk-on rebound play
    """
    if len(args) < 4:
        send_telegram_message(chat_id, (
            "⚠️ <b>Usage:</b> /open SYMBOL SIDE SIZE ENTRY_PRICE [notes=...]\n\n"
            "<b>Example:</b>\n"
            "<code>/open ES long 2 6635</code>\n"
            "<code>/open ES long 2 6635 thesis=risk-on rebound</code>"
        ))
        return

    symbol = args[0].upper()
    side = args[1].lower()
    if side not in ("long", "short"):
        send_telegram_message(chat_id, "⚠️ Side must be <b>long</b> or <b>short</b>.")
        return

    try:
        size = Decimal(args[2])
        entry_price = Decimal(args[3])
    except Exception:
        send_telegram_message(chat_id, "⚠️ SIZE and ENTRY_PRICE must be valid numbers.")
        return

    # Collect anything after the 4 required args as notes
    notes = None
    if len(args) > 4:
        notes = " ".join(args[4:])

    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO positions (symbol, side, size, entry_price, status, notes)
        VALUES (%s, %s, %s, %s, 'open', %s)
        RETURNING id, opened_at;
    """, (symbol, side, size, entry_price, notes))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    pos_id = row[0]
    opened_at = row[1].strftime("%Y-%m-%d %I:%M %p")

    lines = [
        "✅ <b>POSITION OPENED</b>",
        "",
        f"<b>ID:</b> {pos_id}",
        f"<b>Symbol:</b> {esc(symbol)}",
        f"<b>Side:</b> {esc(side)}",
        f"<b>Size:</b> {esc(str(size))}",
        f"<b>Entry:</b> {esc(str(entry_price))}",
        f"<b>Opened:</b> {esc(opened_at)}",
    ]
    if notes:
        lines.append(f"<b>Notes:</b> {esc(notes)}")

    send_telegram_message(chat_id, "\n".join(lines))


# ─── /positions ───────────────────────────────────────────────────────────────

def handle_positions(chat_id: str):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, symbol, side, size, entry_price, opened_at, notes
        FROM positions
        WHERE status = 'open'
        ORDER BY opened_at DESC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        send_telegram_message(chat_id, "📋 No open positions.")
        return

    # Try to get current prices for PnL
    try:
        snapshot = query_latest_snapshot()
        prices = snapshot_to_prices(snapshot) if snapshot else {}
    except Exception:
        prices = {}

    lines = [f"📋 <b>OPEN POSITIONS</b> ({len(rows)})", ""]

    for row in rows:
        pos_id, symbol, side, size, entry_price, opened_at, notes = row
        entry_f = float(entry_price)
        size_f = float(size)
        current = prices.get(symbol)

        pnl_str = "N/A"
        if current is not None:
            if side == "long":
                pnl_points = current - entry_f
            else:
                pnl_points = entry_f - current
            pnl_str = f"{pnl_points:+.2f} pts"

        opened_str = opened_at.strftime("%m/%d %I:%M %p") if opened_at else "?"

        lines.append(
            f"<b>#{pos_id}</b> {esc(symbol)} {esc(side)} ×{esc(str(size))} "
            f"@ {esc(str(entry_price))}"
        )
        if current is not None:
            lines.append(f"   Now: {current:.2f} | PnL: {pnl_str}")
        lines.append(f"   Opened: {esc(opened_str)}")
        if notes:
            lines.append(f"   Notes: {esc(notes[:200])}")
        lines.append("")

    send_telegram_message(chat_id, "\n".join(lines))


# ─── /close ───────────────────────────────────────────────────────────────────

def handle_close(chat_id: str, args: list):
    """
    Usage: /close POSITION_ID
    Example: /close 3
    """
    if len(args) < 1:
        send_telegram_message(chat_id, "⚠️ <b>Usage:</b> /close POSITION_ID\n<b>Example:</b> <code>/close 3</code>")
        return

    try:
        pos_id = int(args[0])
    except ValueError:
        send_telegram_message(chat_id, "⚠️ Position ID must be a number.")
        return

    conn = get_pg_connection()
    cur = conn.cursor()

    # Check it exists and is open
    cur.execute("SELECT symbol, side, size, entry_price FROM positions WHERE id = %s AND status = 'open';", (pos_id,))
    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        send_telegram_message(chat_id, f"⚠️ No open position found with ID <b>{pos_id}</b>.")
        return

    symbol, side, size, entry_price = row

    cur.execute("""
        UPDATE positions SET status = 'closed', closed_at = CURRENT_TIMESTAMP
        WHERE id = %s;
    """, (pos_id,))
    conn.commit()
    cur.close()
    conn.close()

    lines = [
        "🔒 <b>POSITION CLOSED</b>",
        "",
        f"<b>ID:</b> {pos_id}",
        f"<b>Symbol:</b> {esc(str(symbol))}",
        f"<b>Side:</b> {esc(str(side))}",
        f"<b>Size:</b> {esc(str(size))}",
        f"<b>Entry:</b> {esc(str(entry_price))}",
    ]

    send_telegram_message(chat_id, "\n".join(lines))


# ─── /help ────────────────────────────────────────────────────────────────────

def handle_help(chat_id: str):
    lines = [
        "🤖 <b>MARKET AI COMMAND CENTER</b>",
        "",
        "/summary — Current regime, severity, snapshot, AI note",
        "/suggestion — Claude-generated trade ideas (analysis only)",
        "/open SYMBOL SIDE SIZE PRICE [notes] — Log a position",
        "/positions — View open positions with live PnL",
        "/close ID — Close a position by ID",
        "/help — Show this message",
    ]
    send_telegram_message(chat_id, "\n".join(lines))


# ─── Command Router ──────────────────────────────────────────────────────────

def route_command(chat_id: str, text: str):
    """Parse the message text and route to the correct handler."""
    text = text.strip()
    if not text.startswith("/"):
        return

    parts = text.split()
    command = parts[0].lower()
    args = parts[1:]

    # Strip @botname suffix if present (e.g. /summary@MyBot)
    if "@" in command:
        command = command.split("@")[0]

    handlers = {
        "/summary": lambda: handle_summary(chat_id),
        "/suggestion": lambda: handle_suggestion(chat_id),
        "/open": lambda: handle_open(chat_id, args),
        "/positions": lambda: handle_positions(chat_id),
        "/close": lambda: handle_close(chat_id, args),
        "/help": lambda: handle_help(chat_id),
        "/start": lambda: handle_help(chat_id),
    }

    handler = handlers.get(command)
    if handler:
        try:
            handler()
        except Exception as e:
            traceback.print_exc()
            try:
                send_telegram_message(chat_id, f"❌ Error: {esc(str(e)[:500])}")
            except Exception:
                pass


# ─── Long-Polling Loop ───────────────────────────────────────────────────────

def poll_updates():
    """
    Main loop: long-poll Telegram's getUpdates endpoint.
    Processes one update at a time, acknowledges via offset.
    """
    print(f"[{datetime.now()}] Telegram Command Center started.")
    print(f"[{datetime.now()}] Listening for commands...\n")

    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    offset = None

    while True:
        try:
            params = {"timeout": POLL_TIMEOUT, "allowed_updates": ["message"]}
            if offset is not None:
                params["offset"] = offset

            resp = requests.get(f"{base_url}/getUpdates", params=params, timeout=POLL_TIMEOUT + 10)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("ok"):
                print(f"[{datetime.now()}] Telegram API error: {data}")
                time.sleep(POLL_INTERVAL)
                continue

            for update in data.get("result", []):
                update_id = update["update_id"]
                offset = update_id + 1

                message = update.get("message", {})
                chat_id = str(message.get("chat", {}).get("id", ""))
                text = message.get("text", "")

                if not chat_id or not text:
                    continue

                # Optional: restrict to your configured chat ID
                if TELEGRAM_CHAT_ID and chat_id != TELEGRAM_CHAT_ID:
                    print(f"[{datetime.now()}] Ignoring message from unauthorized chat: {chat_id}")
                    continue

                print(f"[{datetime.now()}] Received: {text} (chat: {chat_id})")
                route_command(chat_id, text)

        except requests.exceptions.Timeout:
            # Normal for long-polling — just retry
            continue
        except requests.exceptions.ConnectionError as e:
            print(f"[{datetime.now()}] Connection error: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"[{datetime.now()}] Poll error: {e}")
            traceback.print_exc()
            time.sleep(POLL_INTERVAL)


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        print("ERROR: TELEGRAM_BOT_TOKEN not set in .env")
        exit(1)

    poll_updates()
