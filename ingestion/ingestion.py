import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from utils.hopsworks_conn import get_feature_store

load_dotenv()

API_KEY = os.getenv("API_KEY")

LAT = float(os.getenv("LAT"))
LON = float(os.getenv("LON"))

def fetch_aqi():

    # Pollution API
    pollution_url = "http://api.openweathermap.org/data/2.5/air_pollution"
    pollution = requests.get(pollution_url, params={
        "lat": LAT, "lon": LON, "appid": API_KEY
    }).json()
    #print("FULL API RESPONSE:", pollution)
    comp = pollution["list"][0]["components"]

    # Weather API
    weather_url = "https://api.openweathermap.org/data/2.5/weather"
    weather = requests.get(weather_url, params={
        "lat": LAT, "lon": LON,
        "appid": API_KEY, "units": "metric"
    }).json()

    data = {
        "timestamp": datetime.now(),
        "aqi": pollution["list"][0]["main"]["aqi"],
        "pm2_5": comp.get("pm2_5"),
        "pm10": comp.get("pm10"),
        "no2": comp.get("no2"),
        "so2": comp.get("so2"),
        "co": comp.get("co"),
        "o3": comp.get("o3"),
        "temperature": weather["main"]["temp"],
        "humidity": weather["main"]["humidity"],
        "wind_speed": weather["wind"]["speed"]
    }

    return pd.DataFrame([data])


def run_ingestion():
    fs = get_feature_store()

    fg = fs.get_or_create_feature_group(
        name="aqi_raw_data",
        version=1,
        primary_key=["timestamp"],
        event_time="timestamp"
    )

    df = fetch_aqi().dropna()
    fg.insert(df)

    print(" Data inserted")


if __name__ == "__main__":
    run_ingestion()