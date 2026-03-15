import os
import yfinance as yf
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

SYMBOLS = {
    "ES": "ES=F",
    "NQ": "NQ=F",
    "RTY": "RTY=F",
    "YM": "YM=F",
    "VIX": "^VIX",
    "DXY": "DX-Y.NYB",
    "TNX": "^TNX",
    "GC": "GC=F",
    "CL": "CL=F",
}

def fetch_latest(symbol):
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="2d", interval="5m")

    if hist.empty:
        return None

    last = hist.iloc[-1]
    ts = hist.index[-1].to_pydatetime()

    return {
        "close": float(last["Close"]),
        "volume": float(last["Volume"]) if "Volume" in hist.columns else 0.0,
        "timestamp": ts,
    }

def main():
    client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )

    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        for name, symbol in SYMBOLS.items():
            data = fetch_latest(symbol)

            if data is None:
                print(f"Skipping {name}: no data")
                continue

            point = (
                Point("market_prices")
                .tag("instrument", name)
                .tag("symbol", symbol)
                .field("close", data["close"])
                .field("volume", data["volume"])
                .time(data["timestamp"], WritePrecision.NS)
            )

            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=point
            )

            print(
                f"Wrote {name} ({symbol}) | "
                f"close={data['close']} volume={data['volume']} "
                f"time={data['timestamp']}"
            )

    finally:
        client.close()

if __name__ == "__main__":
    main()
