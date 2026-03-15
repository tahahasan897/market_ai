import subprocess
import time
from datetime import datetime

CHECK_INTERVAL_SECONDS = 300  # 5 minutes


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
        run_step("ingest_market_data", "python3 src/ingest_market_data.py")
        run_step("live_detector", "python3 src/live_detector.py")

        print(f"\n[{datetime.now()}] Sleeping for {CHECK_INTERVAL_SECONDS} seconds...\n")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
