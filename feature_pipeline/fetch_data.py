from utils.hopsworks_conn import get_feature_store
import pandas as pd

def fetch_data():
    fs = get_feature_store()

    # 📂 Show all available feature groups (DEBUG STEP)
    print("📂 Available Feature Groups:")
    for fg in fs.get_feature_groups():
        print("➡️ Name:", fg.name, "| Version:", fg.version)

    # Feature groups
    historical_fg = fs.get_feature_group("aqi_historical", 1)
    realtime_fg = fs.get_feature_group("aqi_raw_data", 1)

    # 🔍 Separate checks
    if historical_fg is None:
        raise ValueError("❌ 'aqi_historical' Feature Group not found")

    if realtime_fg is None:
        raise ValueError("❌ 'aqi_raw_data' Feature Group not found (version 2)")

    print("📥 Fetching historical data...")
    historical_df = historical_fg.select_all().read()

    print("📥 Fetching realtime data v2...")
    realtime_df = realtime_fg.select_all().read()

    # 🧠 STANDARDIZE COLUMNS (IMPORTANT FIX)

    # 1. unify time column
    historical_df["datetime"] = pd.to_datetime(historical_df["datetime"])
    realtime_df["datetime"] = pd.to_datetime(realtime_df["timestamp"])

    # 2. unify temperature column
    if "temperature" in realtime_df.columns:
        realtime_df = realtime_df.rename(columns={"temperature": "temp"})

    # 3. ensure historical also uses temp
    if "temperature" in historical_df.columns:
        historical_df = historical_df.rename(columns={"temperature": "temp"})

    # 4. align columns (important for ML)
    common_cols = [
        "datetime", "temp", "humidity", "wind_speed",
        "aqi", "pm2_5", "pm10", "no2", "so2", "co", "o3"
    ]

    historical_df = historical_df[common_cols]
    realtime_df = realtime_df[common_cols]

    # 🔗 Merge datasets
    df = pd.concat([historical_df, realtime_df], ignore_index=True)

    # 📊 Sort by time (VERY IMPORTANT for time series ML)
    df = df.sort_values("datetime")

    print("✅ Data fetched successfully:", df.shape)

    return df