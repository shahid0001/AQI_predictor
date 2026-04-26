import requests
import pandas as pd
from ingestion.config import LAT, LON, START_DATE, END_DATE


def fetch_weather():
    print("🌤 Fetching weather data from Open-Meteo...")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={LAT}"
        f"&longitude={LON}"
        f"&start_date={START_DATE}"
        f"&end_date={END_DATE}"
        f"&hourly=temperature_2m,relative_humidity_2m,windspeed_10m"
    )

    try:
        res = requests.get(url)
        res.raise_for_status()  # Catch HTTP errors

        data = res.json()

        if "hourly" not in data:
            raise ValueError("Invalid API response")

        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temp": data["hourly"]["temperature_2m"],
            "humidity": data["hourly"]["relative_humidity_2m"],
            "wind_speed": data["hourly"]["windspeed_10m"]
        })

        if df.empty:
            raise ValueError("No weather data fetched")

        # Convert time column
        df["time"] = pd.to_datetime(df["time"])

        # Sort (VERY IMPORTANT for time series)
        df = df.sort_values("time")

        print(f" Weather data fetched: {df.shape}")

        return df

    except Exception as e:
        print("Error fetching weather data:", e)
        return None


if __name__ == "__main__":
    df = fetch_weather()
    print(df.head())