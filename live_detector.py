import os
import requests
import psycopg2
import anthropic
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from alerts import format_short_telegram_alert, send_telegram_message
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
OLLAMA_URL = os.getenv("OLLAMA_URL")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")

MAX_AGE_MINUTES = {
    "ES": 15,
    "NQ": 15,
    "RTY": 15,
    "YM": 15,
    "VIX": 20,
    "DXY": 20,
    "TNX": 90,
    "GC": 20,
    "CL": 20,
}

EASTERN = ZoneInfo("America/New_York")


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def setup_table():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS live_market_events (
            id SERIAL PRIMARY KEY,
            event_type TEXT,
            regime TEXT,
            flags TEXT,
            summary TEXT,
            local_summary TEXT,
            claude_summary TEXT,
            escalation_used BOOLEAN DEFAULT FALSE,
            severity_score INTEGER DEFAULT 0,
            severity_label TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    cur.execute("""
        ALTER TABLE live_market_events
        ADD COLUMN IF NOT EXISTS local_summary TEXT,
        ADD COLUMN IF NOT EXISTS claude_summary TEXT,
        ADD COLUMN IF NOT EXISTS escalation_used BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS severity_score INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS severity_label TEXT,
        ADD COLUMN IF NOT EXISTS status TEXT;
    """)
    conn.commit()

    cur.close()
    conn.close()


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
    return {instrument: payload["price"] for instrument, payload in snapshot.items()}


def is_market_closed_now():
    """
    Basic CME-style guard for your use case:
    - Closed most of Saturday
    - Reopens Sunday evening ET
    - For simplicity, treat Sunday before 6:00 PM ET as closed
    """
    now_et = datetime.now(EASTERN)
    weekday = now_et.weekday()  # Monday=0 ... Sunday=6
    hour = now_et.hour
    minute = now_et.minute

    # Saturday
    if weekday == 5:
        return True

    # Sunday before 6 PM ET
    if weekday == 6 and (hour < 18):
        return True

    return False


def get_stale_instruments(snapshot):
    now = datetime.now(timezone.utc)
    stale = []

    for instrument, payload in snapshot.items():
        ts = payload["time"]
        max_age = MAX_AGE_MINUTES.get(instrument, 30)
        age_minutes = (now - ts).total_seconds() / 60.0

        if age_minutes > max_age:
            stale.append({
                "instrument": instrument,
                "age_minutes": round(age_minutes, 1),
                "allowed_minutes": max_age,
            })

    return stale


def detect_market_regime(prices):
    flags = []

    es = prices.get("ES")
    nq = prices.get("NQ")
    vix = prices.get("VIX")
    dxy = prices.get("DXY")
    tnx = prices.get("TNX")
    gc = prices.get("GC")
    cl = prices.get("CL")

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

    event_triggered = regime != "neutral"

    return event_triggered, regime, flags


def compute_severity_score(flags):
    weights = {
        "high_vix": 2,
        "strong_dollar": 1,
        "high_yields": 2,
        "strong_gold": 1,
        "firm_oil": 1,
        "nq_soft": 1,
        "es_soft": 1,
    }
    return sum(weights.get(flag, 0) for flag in flags)


def severity_label_from_score(score):
    if score >= 7:
        return "extreme"
    if score >= 5:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def get_last_event():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            regime,
            flags,
            claude_summary,
            local_summary,
            escalation_used,
            severity_score,
            severity_label,
            status,
            created_at
        FROM live_market_events
        ORDER BY created_at DESC
        LIMIT 1;
    """)

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        return None

    return {
        "id": row[0],
        "regime": row[1],
        "flags": row[2],
        "claude_summary": row[3],
        "local_summary": row[4],
        "escalation_used": row[5],
        "severity_score": row[6],
        "severity_label": row[7],
        "status": row[8],
        "created_at": row[9],
    }


def classify_status(current_regime, current_score, current_flags, last_event):
    if not last_event:
        return "NEW_REGIME"

    last_regime = last_event["regime"]
    last_score = last_event["severity_score"] or 0
    last_flags_text = last_event["flags"] or ""
    last_flags = set(flag.strip() for flag in last_flags_text.split(",") if flag.strip())
    current_flag_set = set(current_flags)

    if current_regime != last_regime:
        return "NEW_REGIME"

    score_diff = current_score - last_score

    if score_diff >= 2:
        return "REGIME_WORSENING"

    if score_diff <= -2:
        return "REGIME_EASING"

    if current_flag_set != last_flags and abs(score_diff) >= 1:
        if score_diff > 0:
            return "REGIME_WORSENING"
        if score_diff < 0:
            return "REGIME_EASING"

    return "ONGOING_REGIME"


def should_refresh_ai(status, last_event):
    if status in {"NEW_REGIME", "REGIME_WORSENING", "REGIME_EASING"}:
        return True

    if not last_event:
        return True

    age = datetime.now() - last_event["created_at"]
    if age.total_seconds() >= 4 * 3600:
        return True

    return False


def get_reusable_ai_note(last_event):
    if not last_event:
        return None

    if last_event.get("claude_summary"):
        return last_event["claude_summary"]

    if last_event.get("local_summary"):
        return last_event["local_summary"]

    return None


def should_escalate_to_claude(regime, flags):
    flag_set = set(flags)

    if regime == "risk_off_macro_pressure" and len(flag_set) >= 5:
        return True

    if {"high_vix", "strong_dollar", "high_yields", "strong_gold"}.issubset(flag_set):
        return True

    return False


def summarize_with_ollama(prices, regime, flags):
    prompt = f"""
You are a market research assistant.

Analyze this live cross-asset snapshot and write:
1. A 3-bullet summary
2. One short paragraph explaining the likely regime
3. Two risks to monitor next

Use only the values shown below.
Do not invent prices.
Do not rename instruments.
If you mention a number, copy it directly from the snapshot.

Cross-asset snapshot:
ES futures: {prices.get('ES')}
NQ futures: {prices.get('NQ')}
RTY futures: {prices.get('RTY')}
YM futures: {prices.get('YM')}
VIX: {prices.get('VIX')}
DXY: {prices.get('DXY')}
10Y Yield (TNX): {prices.get('TNX')}
Gold futures (GC): {prices.get('GC')}
Crude oil futures (CL): {prices.get('CL')}

Detected regime: {regime}
Flags: {", ".join(flags)}
"""

    payload = {
        "model": "llama3",
        "prompt": prompt,
        "stream": False
    }

    response = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=120)
    response.raise_for_status()
    return response.json()["response"]


def summarize_with_claude(prices, regime, flags, local_summary):
    if not ANTHROPIC_API_KEY:
        return None

    client = anthropic.Anthropic(
        api_key=ANTHROPIC_API_KEY,
        timeout=60.0,
    )

    prompt = f"""
You are a professional cross-asset market analyst.

A local model already produced this first-pass summary:
{local_summary}

Now produce a higher-quality analyst note.

Requirements:
- Use only the market data shown below
- Do not invent prices or facts
- Interpret the likely macro regime
- Explain what the cross-asset relationships may imply
- Give 2 scenario paths
- Give 3 concrete risks to monitor
- Keep it concise but more professional than the local summary

Market snapshot:
ES futures: {prices.get('ES')}
NQ futures: {prices.get('NQ')}
RTY futures: {prices.get('RTY')}
YM futures: {prices.get('YM')}
VIX: {prices.get('VIX')}
DXY: {prices.get('DXY')}
10Y Yield (TNX): {prices.get('TNX')}
Gold futures (GC): {prices.get('GC')}
Crude oil futures (CL): {prices.get('CL')}

Detected regime: {regime}
Flags: {", ".join(flags)}
"""

    message = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=700,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    parts = []
    for block in message.content:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)

    return "\n".join(parts).strip()


def save_event(regime, flags, local_summary, claude_summary, escalation_used, severity_score, severity_label, status):
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO live_market_events (
            event_type,
            regime,
            flags,
            summary,
            local_summary,
            claude_summary,
            escalation_used,
            severity_score,
            severity_label,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        "live_cross_asset_event",
        regime,
        ", ".join(sorted(flags)),
        claude_summary if claude_summary else local_summary,
        local_summary,
        claude_summary,
        escalation_used,
        severity_score,
        severity_label,
        status,
    ))

    conn.commit()
    cur.close()
    conn.close()


def should_save_ongoing_checkpoint(last_event, checkpoint_hours=2):
    if not last_event:
        return True

    age = datetime.now() - last_event["created_at"]
    return age.total_seconds() >= checkpoint_hours * 3600


def get_last_meaningful_event():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            regime,
            flags,
            claude_summary,
            local_summary,
            escalation_used,
            severity_score,
            severity_label,
            status,
            created_at
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
        "id": row[0],
        "regime": row[1],
        "flags": row[2],
        "claude_summary": row[3],
        "local_summary": row[4],
        "escalation_used": row[5],
        "severity_score": row[6],
        "severity_label": row[7],
        "status": row[8],
        "created_at": row[9],
    }


def main():
    setup_table()

    snapshot = query_latest_snapshot()
    if not snapshot:
        print("No snapshot data found in InfluxDB.")
        return

    prices = snapshot_to_prices(snapshot)
    print("Latest prices:", prices)

    stale = get_stale_instruments(snapshot)
    market_closed = is_market_closed_now()

    if stale:
        if market_closed:
            print("Market appears closed right now. Latest snapshot is stale, so analysis is skipped.")
            for item in stale:
                print(
                    f" - {item['instrument']}: "
                    f"age={item['age_minutes']} min, "
                    f"allowed={item['allowed_minutes']} min"
                )
            return

        print("Stale instruments detected:")
        for item in stale:
            print(
                f" - {item['instrument']}: "
                f"age={item['age_minutes']} min, "
                f"allowed={item['allowed_minutes']} min"
            )

        print("Skipping analysis because snapshot is stale.")
        return

    triggered, regime, flags = detect_market_regime(prices)

    if not triggered:
        print("No live event triggered.")
        print("Flags:", flags)
        return

    severity_score = compute_severity_score(flags)
    severity_label = severity_label_from_score(severity_score)

    last_event = get_last_meaningful_event()
    status = classify_status(regime, severity_score, flags, last_event)

    print("Event triggered.")
    print("Regime:", regime)
    print("Flags:", flags)
    print("Severity score:", severity_score)
    print("Severity label:", severity_label)
    print("Status:", status)

    local_summary = None
    claude_summary = None
    escalation_used = False
    ai_note_for_alert = None

    refresh_ai = should_refresh_ai(status, last_event)
    reusable_note = get_reusable_ai_note(last_event)

    if refresh_ai:
        print("\nAI refresh required.")

        local_summary = summarize_with_ollama(prices, regime, flags)
        print("\nLOCAL LLM SUMMARY:\n")
        print(local_summary)

        if should_escalate_to_claude(regime, flags):
            print("\nEscalating to Claude...\n")
            try:
                claude_summary = summarize_with_claude(prices, regime, flags, local_summary)
                escalation_used = True
                print("CLAUDE SUMMARY:\n")
                print(claude_summary)
            except Exception as e:
                print(f"Claude escalation failed: {e}")
                claude_summary = None
                escalation_used = False

        ai_note_for_alert = claude_summary if claude_summary else local_summary

    else:
        print("\nReusing previous analysis. No new AI call.")
        ai_note_for_alert = reusable_note
        local_summary = last_event["local_summary"] if last_event else None
        claude_summary = last_event["claude_summary"] if last_event else None
        escalation_used = last_event["escalation_used"] if last_event else False

    should_save_event = status in {"NEW_REGIME", "REGIME_WORSENING", "REGIME_EASING"}

    if status == "ONGOING_REGIME":
        should_save_event = should_save_ongoing_checkpoint(last_event, checkpoint_hours=2)

    if should_save_event:
        save_event(
            regime=regime,
            flags=flags,
            local_summary=local_summary,
            claude_summary=claude_summary,
            escalation_used=escalation_used,
            severity_score=severity_score,
            severity_label=severity_label,
            status=status,
        )
        print("\nSaved to PostgreSQL successfully.")
    else:
        print("\nSkipping PostgreSQL save for ongoing regime.")

    should_send_alert = status != "ONGOING_REGIME"

    if should_send_alert:
        try:
            telegram_message = format_short_telegram_alert(
                status=status,
                regime=regime,
                severity_label=severity_label,
                severity_score=severity_score,
                flags=flags,
                prices=prices,
                escalation_used=escalation_used,
                ai_note=ai_note_for_alert,
            )
            send_telegram_message(telegram_message)
        except Exception as e:
            print(f"Telegram alert failed: {e}")
    else:
        print("Ongoing regime only. Skipping Telegram alert.")


if __name__ == "__main__":
    main()
