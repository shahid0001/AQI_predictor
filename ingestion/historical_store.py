import pandas as pd

from ingestion.pollution_data import fetch_pollution
from ingestion.weather_data import fetch_weather
from ingestion.merge_data import merge_data
from ingestion.upload_feature_store import upload_to_hopsworks


def run_historical_store():
    print("🚀 Starting Historical Data Store Pipeline...")

    # Step 1: Fetch Data
    print("📡 Fetching pollution data...")
    pollution_df = fetch_pollution()

    print("🌤 Fetching weather data...")
    weather_df = fetch_weather()

    # Step 2: Validate
    if pollution_df is None or weather_df is None:
        raise ValueError("❌ Data fetch failed")

    print("✅ Data fetched successfully")

    # Step 3: Rename columns for consistency
    pollution_df = pollution_df.rename(columns={"time": "datetime"})
    weather_df = weather_df.rename(columns={"time": "datetime"})

    # Step 4: Convert datetime properly (IMPORTANT)
    pollution_df["datetime"] = pd.to_datetime(pollution_df["datetime"])
    weather_df["datetime"] = pd.to_datetime(weather_df["datetime"])

    # Step 5: SORT + REMOVE DUPLICATES (VERY IMPORTANT FIX)
    pollution_df = pollution_df.sort_values("datetime")
    pollution_df = pollution_df.drop_duplicates(subset=["datetime"], keep="last")

    weather_df = weather_df.sort_values("datetime")
    weather_df = weather_df.drop_duplicates(subset=["datetime"], keep="last")

    print("Pollution & Weather cleaned (duplicates removed)")

    # Step 6: Merge
    merged_df = merge_data(weather_df, pollution_df)

    # Step 7: FINAL CLEAN (CRITICAL STEP)
    merged_df["datetime"] = pd.to_datetime(merged_df["datetime"])
    merged_df = merged_df.sort_values("datetime")
    merged_df = merged_df.drop_duplicates(subset=["datetime"], keep="last")

    print(f"🔗 Data merged: {merged_df.shape}")

    # Step 8: Save locally
    merged_df.to_csv("data/historical_merged2.csv", index=False)
    print(" Saved locally")

    # Step 9: Upload to Hopsworks
    upload_to_hopsworks(merged_df)

    print("Uploaded to Hopsworks successfully")

    print(" Historical Store Pipeline Completed!")

    return merged_df


if __name__ == "__main__":
    run_historical_store()