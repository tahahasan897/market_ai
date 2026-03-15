import os
import html
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def esc(text) -> str:
    if text is None:
        return ""
    return html.escape(str(text))


def format_short_telegram_alert(
    status,
    regime,
    severity_label,
    severity_score,
    flags,
    prices,
    escalation_used=False,
    ai_note=None,
):
    timestamp = datetime.now().strftime("%Y-%m-%d %I:%M %p")

    lines = [
        f"🚨 <b>{esc(status)}</b>",
        f"<b>Time:</b> {esc(timestamp)}",
        f"<b>Regime:</b> {esc(regime)}",
        f"<b>Severity:</b> {esc(severity_label)} ({esc(severity_score)})",
        "",
        "<b>Snapshot:</b>",
        f"ES: {esc(prices.get('ES'))} | NQ: {esc(prices.get('NQ'))}",
        f"VIX: {esc(prices.get('VIX'))} | DXY: {esc(prices.get('DXY'))} | TNX: {esc(prices.get('TNX'))}",
        f"GC: {esc(prices.get('GC'))} | CL: {esc(prices.get('CL'))}",
        "",
        f"<b>Flags:</b> {esc(', '.join(sorted(flags)))}",
        f"<b>Claude Used:</b> {esc(escalation_used)}",
    ]

    if ai_note:
        lines.extend([
            "",
            "<b>AI Note:</b>",
            esc(ai_note[:700])
        ])

    return "\n".join(lines)


def send_telegram_message(message: str):
    if not telegram_enabled():
        print("Telegram is not configured. Skipping Telegram alert.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    print("Telegram alert sent successfully.")
