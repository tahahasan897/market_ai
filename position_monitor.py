"""
Position Monitor for the Market AI Assistant.

Runs on a schedule (called from run_monitor.py or standalone).
For each open position:
    - Fetches current market price from InfluxDB
    - Computes unrealized PnL
    - Checks regime alignment
    - Detects adverse moves and severity changes
    - Generates AI position review when warranted
    - Sends Telegram alerts for positions needing attention

Does NOT auto-trade or place orders. Informational alerts only.
"""

import os
import html
import requests
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

# ─── Alert Thresholds ────────────────────────────────────────────────────────

# Point thresholds for adverse move alerts (per instrument)
# These are soft risk alert levels — the trader does NOT use hard stops
ADVERSE_MOVE_THRESHOLDS = {
    "ES": 30,      # ~0.5% on ES
    "NQ": 150,     # ~0.6% on NQ
    "RTY": 20,     # ~0.8% on RTY
    "YM": 250,     # ~0.5% on YM
    "GC": 40,      # ~0.8% on GC
    "CL": 2.0,     # ~2.5% on CL
}

# Default threshold if instrument not listed above
DEFAULT_ADVERSE_THRESHOLD = 50

# ─── Helpers ─────────────────────────────────────────────────────────────────

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


def send_telegram_message(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured. Skipping alert.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()


def query_latest_snapshot():
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


# ─── Position Queries ────────────────────────────────────────────────────────

def get_open_positions():
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, symbol, side, size, entry_price, opened_at, notes
        FROM positions
        WHERE status = 'open'
        ORDER BY opened_at ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    positions = []
    for row in rows:
        positions.append({
            "id": row[0],
            "symbol": row[1],
            "side": row[2],
            "size": float(row[3]),
            "entry_price": float(row[4]),
            "opened_at": row[5],
            "notes": row[6],
        })
    return positions


def get_last_alert_time(position_id):
    """Check when we last sent an alert for this position to avoid spam."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT last_alert_at FROM position_alerts
        WHERE position_id = %s
        ORDER BY last_alert_at DESC
        LIMIT 1;
    """)
    # If the table doesn't exist yet, return None gracefully
    try:
        cur.execute("""
            SELECT last_alert_at FROM position_alerts
            WHERE position_id = %s
            ORDER BY last_alert_at DESC
            LIMIT 1;
        """, (position_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception:
        cur.close()
        conn.close()
        return None


# ─── Position Alert Tracking ─────────────────────────────────────────────────

def setup_alert_tracking_table():
    """Create table to track when alerts were last sent per position."""
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS position_alerts (
            id SERIAL PRIMARY KEY,
            position_id INTEGER NOT NULL REFERENCES positions(id),
            alert_type TEXT NOT NULL,
            alert_message TEXT,
            pnl_points NUMERIC,
            regime TEXT,
            last_alert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def get_last_alert_for_position(position_id):
    """Get the most recent alert for a position."""
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT alert_type, pnl_points, regime, last_alert_at
            FROM position_alerts
            WHERE position_id = %s
            ORDER BY last_alert_at DESC
            LIMIT 1;
        """, (position_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return None
        return {
            "alert_type": row[0],
            "pnl_points": float(row[1]) if row[1] else None,
            "regime": row[2],
            "last_alert_at": row[3],
        }
    except Exception:
        cur.close()
        conn.close()
        return None


def save_alert_record(position_id, alert_type, alert_message, pnl_points, regime):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO position_alerts (position_id, alert_type, alert_message, pnl_points, regime)
        VALUES (%s, %s, %s, %s, %s);
    """, (position_id, alert_type, alert_message, pnl_points, regime))
    conn.commit()
    cur.close()
    conn.close()


# ─── Position Evaluation ─────────────────────────────────────────────────────

def evaluate_position(position, current_price, regime, flags, severity_score, severity_label):
    """
    Evaluate a single position against current market conditions.
    Returns a dict with assessment details and whether an alert is warranted.
    """
    symbol = position["symbol"]
    side = position["side"]
    entry = position["entry_price"]
    size = position["size"]

    # Compute PnL
    if side == "long":
        pnl_points = current_price - entry
    else:
        pnl_points = entry - current_price

    # Check for adverse move
    threshold = ADVERSE_MOVE_THRESHOLDS.get(symbol, DEFAULT_ADVERSE_THRESHOLD)
    adverse_move = pnl_points < -threshold

    # Check regime alignment
    regime_conflict = False
    conflict_reason = None

    if regime == "risk_off_macro_pressure":
        # In risk-off, longs on equity indices are conflicting
        if side == "long" and symbol in ("ES", "NQ", "RTY", "YM"):
            regime_conflict = True
            conflict_reason = "Long equity position conflicts with risk-off macro pressure regime"

        # In risk-off, shorts on safe havens are conflicting
        if side == "short" and symbol in ("GC",):
            regime_conflict = True
            conflict_reason = "Short gold conflicts with risk-off environment (gold is a safe haven)"

        # In risk-off, oil shorts may actually align
        if side == "long" and symbol == "CL" and "firm_oil" not in set(flags):
            regime_conflict = True
            conflict_reason = "Long oil may face headwinds in risk-off without firm oil signal"

    elif regime == "neutral":
        # In neutral, no strong regime conflict — positions are generally okay
        pass

    # Determine alert type
    alert_types = []

    if adverse_move:
        alert_types.append("ADVERSE_MOVE")

    if regime_conflict:
        alert_types.append("REGIME_CONFLICT")

    if severity_score >= 5 and side == "long" and symbol in ("ES", "NQ", "RTY", "YM"):
        alert_types.append("HIGH_SEVERITY")

    # Check if PnL has significantly worsened since last alert
    needs_alert = len(alert_types) > 0

    return {
        "position": position,
        "current_price": current_price,
        "pnl_points": pnl_points,
        "adverse_move": adverse_move,
        "regime_conflict": regime_conflict,
        "conflict_reason": conflict_reason,
        "alert_types": alert_types,
        "needs_alert": needs_alert,
        "regime": regime,
        "severity_label": severity_label,
        "severity_score": severity_score,
    }


def should_send_alert(position_id, current_alert_types, current_pnl, regime):
    """
    Avoid alert spam. Only send if:
    - No previous alert exists for this position
    - Alert type has changed (e.g. new regime conflict)
    - PnL has worsened significantly since last alert
    - At least 30 minutes since last alert
    """
    last_alert = get_last_alert_for_position(position_id)

    if not last_alert:
        return True

    time_since = datetime.now() - last_alert["last_alert_at"]
    minutes_since = time_since.total_seconds() / 60.0

    # Always wait at least 30 minutes between alerts for same position
    if minutes_since < 30:
        return False

    # Alert if regime changed
    if last_alert["regime"] != regime:
        return True

    # Alert if PnL worsened significantly (more than 50% worse)
    if last_alert["pnl_points"] is not None and current_pnl < last_alert["pnl_points"] * 1.5:
        return True

    # Alert if new alert type appeared
    last_type = last_alert["alert_type"] or ""
    for atype in current_alert_types:
        if atype not in last_type:
            return True

    # Re-alert every 2 hours if issue persists
    if minutes_since >= 120:
        return True

    return False


# ─── AI Position Review ──────────────────────────────────────────────────────

POSITION_REVIEW_PROMPT = """You are a risk-aware market analyst reviewing open positions for a swing trader.

The trader does NOT use hard stop-loss orders on the exchange. Instead, they rely on soft risk alerts.

Review the following position against current market conditions and provide a brief assessment.

Rules:
- Do NOT issue trade orders
- Be direct and concise (under 200 words)
- Focus on: regime alignment, risk level, and what to watch
- If the position conflicts with the regime, say so clearly
- If the position is favorable, confirm why
- Give one concrete action suggestion (reduce, hold, add, or monitor a level)

Position:
  Symbol: {symbol}
  Side: {side}
  Size: {size}
  Entry: {entry}
  Current: {current}
  PnL: {pnl} points
  Opened: {opened}
  Notes: {notes}

Market conditions:
  Regime: {regime}
  Severity: {severity_label} ({severity_score})
  Flags: {flags}

Snapshot:
  ES: {es} | NQ: {nq} | RTY: {rty} | YM: {ym}
  VIX: {vix} | DXY: {dxy} | TNX: {tnx}
  GC: {gc} | CL: {cl}
"""


def generate_ai_review(position, eval_result, prices, flags):
    """Call Claude for a position-specific review. Only used for high-priority alerts."""
    if not ANTHROPIC_API_KEY:
        return None

    prompt = POSITION_REVIEW_PROMPT.format(
        symbol=position["symbol"],
        side=position["side"],
        size=position["size"],
        entry=position["entry_price"],
        current=eval_result["current_price"],
        pnl=f"{eval_result['pnl_points']:+.2f}",
        opened=position["opened_at"].strftime("%Y-%m-%d %I:%M %p") if position["opened_at"] else "N/A",
        notes=position["notes"] or "none",
        regime=eval_result["regime"],
        severity_label=eval_result["severity_label"],
        severity_score=eval_result["severity_score"],
        flags=", ".join(sorted(flags)) if flags else "none",
        es=prices.get("ES"), nq=prices.get("NQ"),
        rty=prices.get("RTY"), ym=prices.get("YM"),
        vix=prices.get("VIX"), dxy=prices.get("DXY"),
        tnx=prices.get("TNX"), gc=prices.get("GC"),
        cl=prices.get("CL"),
    )

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=60.0)
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        parts = []
        for block in message.content:
            if getattr(block, "type", None) == "text":
                parts.append(block.text)
        return "\n".join(parts).strip()
    except Exception as e:
        print(f"Claude position review failed: {e}")
        return None


# ─── Alert Formatting ────────────────────────────────────────────────────────

def format_position_alert(eval_result, ai_review=None):
    """Format a Telegram alert for a position that needs attention."""
    pos = eval_result["position"]
    pnl = eval_result["pnl_points"]
    alert_types = eval_result["alert_types"]

    # Choose emoji based on severity
    if "ADVERSE_MOVE" in alert_types and "REGIME_CONFLICT" in alert_types:
        emoji = "🔴"
    elif "ADVERSE_MOVE" in alert_types:
        emoji = "🟠"
    elif "REGIME_CONFLICT" in alert_types:
        emoji = "🟡"
    else:
        emoji = "⚠️"

    alert_labels = " + ".join(alert_types)

    lines = [
        f"{emoji} <b>POSITION ALERT — {esc(alert_labels)}</b>",
        "",
        f"<b>#{pos['id']}</b> {esc(pos['symbol'])} {esc(pos['side'])} ×{esc(str(pos['size']))} @ {esc(str(pos['entry_price']))}",
        f"<b>Current:</b> {eval_result['current_price']:.2f}",
        f"<b>PnL:</b> {pnl:+.2f} pts",
        "",
        f"<b>Regime:</b> {esc(eval_result['regime'])}",
        f"<b>Severity:</b> {esc(eval_result['severity_label'])} ({esc(eval_result['severity_score'])})",
    ]

    if eval_result["conflict_reason"]:
        lines.extend(["", f"<b>Conflict:</b> {esc(eval_result['conflict_reason'])}"])

    if ai_review:
        lines.extend([
            "",
            "<b>── AI Review ──</b>",
            esc(ai_review[:1000]),
        ])

    lines.extend(["", "<i>Soft risk alert — not a trade order.</i>"])

    return "\n".join(lines)


def format_all_clear(positions, prices, regime, severity_label, severity_score):
    """Optional quiet summary when all positions are healthy."""
    lines = [
        "✅ <b>POSITION CHECK — ALL CLEAR</b>",
        f"<b>Time:</b> {esc(datetime.now(EASTERN).strftime('%Y-%m-%d %I:%M %p ET'))}",
        f"<b>Regime:</b> {esc(regime)} | <b>Severity:</b> {esc(severity_label)} ({esc(severity_score)})",
        "",
    ]

    for pos in positions:
        symbol = pos["symbol"]
        current = prices.get(symbol)
        if current is not None:
            entry = pos["entry_price"]
            if pos["side"] == "long":
                pnl = current - entry
            else:
                pnl = entry - current
            lines.append(
                f"  #{pos['id']} {esc(symbol)} {esc(pos['side'])} ×{esc(str(pos['size']))} "
                f"@ {esc(str(entry))} → {current:.2f} ({pnl:+.2f})"
            )

    return "\n".join(lines)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    setup_alert_tracking_table()

    # Get open positions
    positions = get_open_positions()
    if not positions:
        print(f"[{datetime.now()}] No open positions to monitor.")
        return

    print(f"[{datetime.now()}] Monitoring {len(positions)} open position(s)...")

    # Get current market data
    snapshot = query_latest_snapshot()
    if not snapshot:
        print(f"[{datetime.now()}] No market data available. Skipping position monitor.")
        return

    prices = snapshot_to_prices(snapshot)
    regime, flags = detect_market_regime(prices)
    severity_score = compute_severity_score(flags)
    severity_label = severity_label_from_score(severity_score)

    print(f"[{datetime.now()}] Regime: {regime} | Severity: {severity_label} ({severity_score})")

    alerts_sent = 0
    positions_checked = 0

    for pos in positions:
        symbol = pos["symbol"]
        current_price = prices.get(symbol)

        if current_price is None:
            print(f"  #{pos['id']} {symbol}: no current price available, skipping.")
            continue

        positions_checked += 1

        # Evaluate position
        eval_result = evaluate_position(
            position=pos,
            current_price=current_price,
            regime=regime,
            flags=flags,
            severity_score=severity_score,
            severity_label=severity_label,
        )

        pnl = eval_result["pnl_points"]
        print(
            f"  #{pos['id']} {symbol} {pos['side']} ×{pos['size']} "
            f"@ {pos['entry_price']} → {current_price:.2f} "
            f"(PnL: {pnl:+.2f}) "
            f"alerts: {eval_result['alert_types'] or 'none'}"
        )

        if not eval_result["needs_alert"]:
            continue

        # Check if we should actually send (avoid spam)
        if not should_send_alert(pos["id"], eval_result["alert_types"], pnl, regime):
            print(f"    Suppressed (too recent or unchanged).")
            continue

        # Generate AI review for high-priority alerts
        ai_review = None
        if "ADVERSE_MOVE" in eval_result["alert_types"] or "REGIME_CONFLICT" in eval_result["alert_types"]:
            print(f"    Generating AI review...")
            ai_review = generate_ai_review(pos, eval_result, prices, flags)

        # Format and send alert
        alert_message = format_position_alert(eval_result, ai_review)

        try:
            send_telegram_message(alert_message)
            alerts_sent += 1
            print(f"    Alert sent.")
        except Exception as e:
            print(f"    Failed to send alert: {e}")
            continue

        # Record the alert to prevent spam
        save_alert_record(
            position_id=pos["id"],
            alert_type=" + ".join(eval_result["alert_types"]),
            alert_message=alert_message[:500],
            pnl_points=pnl,
            regime=regime,
        )

    print(f"\n[{datetime.now()}] Position monitor complete. "
          f"Checked: {positions_checked}, Alerts sent: {alerts_sent}")


if __name__ == "__main__":
    main()
