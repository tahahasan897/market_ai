import subprocess
import time
from datetime import datetime
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
CLOSED_CHECK_INTERVAL_SECONDS = 1800  # 30 minutes

def is_market_closed_now():
    """
    Simple market-hours guard for your futures-monitor workflow.

    Treat as closed:
    - Saturday
    - Sunday before 6:00 PM ET

    Otherwise open enough to resume polling.
    """
    now_et = datetime.now(EASTERN)
    weekday = now_et.weekday()  # Monday=0 ... Sunday=6
    hour = now_et.hour

    if weekday == 5:  # Saturday
        return True

    if weekday == 6 and hour < 18:  # Sunday before 6 PM ET
        return True

    return False

def run_step(label, command):
    print(f"\n[{datetime.now()}] Running: {label}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"[{datetime.now()}] ERROR in step: {label}")
    else:
        print(f"[{datetime.now()}] Finished: {label}")


def main():
    print("Starting market monitor loop...")

    while True:
        if is_market_closed_now():
            print(
                f"\n[{datetime.now()}] Market appears closed. "
                f"Sleeping for {CLOSED_CHECK_INTERVAL_SECONDS} seconds...\n"
            )
            time.sleep(CLOSED_CHECK_INTERVAL_SECONDS)
            continue

        run_step("ingest_market_data", "python3 src/ingest_market_data.py")
        run_step("live_detector", "python3 src/live_detector.py")

        print(f"\n[{datetime.now()}] Sleeping for {CHECK_INTERVAL_SECONDS} seconds...\n")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
