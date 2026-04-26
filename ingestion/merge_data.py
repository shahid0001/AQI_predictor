def merge_data(weather_df, pollution_df):
    print("🔗 Merging weather and pollution data...")

    # Rename time → datetime (if needed)
    if "time" in weather_df.columns:
        weather_df = weather_df.rename(columns={"time": "datetime"})

    if "time" in pollution_df.columns:
        pollution_df = pollution_df.rename(columns={"time": "datetime"})

    # Align timestamps to hourly
    weather_df["datetime"] = weather_df["datetime"].dt.floor("h")
    pollution_df["datetime"] = pollution_df["datetime"].dt.floor("h")

    # Merge datasets
    df = weather_df.merge(
        pollution_df,
        on="datetime",
        how="inner"
    )

    print(f"✅ Merged shape: {df.shape}")

    return df