import requests
import pandas as pd
from datetime import datetime, timedelta
from ingestion.config import API_KEY, LAT, LON, START, END


def fetch_pollution():
    print("🌫 Fetching pollution data (safe chunk mode)...")

    all_rows = []

    current = datetime.fromtimestamp(START)
    end_date = datetime.fromtimestamp(END)

    while current < end_date:
        next_day = current + timedelta(days=1)

        start_ts = int(current.timestamp())
        end_ts = int(next_day.timestamp())

        url = "http://api.openweathermap.org/data/2.5/air_pollution/history"

        params = {
            "lat": LAT,
            "lon": LON,
            "start": start_ts,
            "end": end_ts,
            "appid": API_KEY
        }

        try:
            res = requests.get(url, params=params, timeout=10)

            # IMPORTANT FIX
            if res.status_code != 200:
                print(f" Skipping {current.date()} (status {res.status_code})")
                current = next_day
                continue

            # SAFE JSON PARSE
            try:
                data = res.json()
            except:
                print(f" Invalid JSON on {current.date()} → skipping")
                current = next_day
                continue

            if "list" not in data:
                print(f" No data on {current.date()}")
                current = next_day
                continue

            for item in data["list"]:
                all_rows.append({
                    "dt": item["dt"],
                    "aqi": item["main"]["aqi"],
                    "pm2_5": item["components"].get("pm2_5"),
                    "pm10": item["components"].get("pm10"),
                    "no2": item["components"].get("no2"),
                    "so2": item["components"].get("so2"),
                    "co": item["components"].get("co"),
                    "o3": item["components"].get("o3"),
                })

        except Exception as e:
            print(f"❌ Error on {current.date()}:", e)

        current = next_day

    df = pd.DataFrame(all_rows)

    if df.empty:
        print("No pollution data fetched")
        return None

    df["time"] = pd.to_datetime(df["dt"], unit="s")
    df = df.sort_values("time")
    df = df.drop(columns=["dt"])

    print(f" Pollution data fetched: {df.shape}")

    return df