from feature_pipeline.fetch_data import fetch_data

def export_csv():
    df = fetch_data()

    # Save raw merged data
    df.to_csv("aqi_merged_raw.csv", index=False)

    print("✅ CSV exported for Colab:", df.shape)

if __name__ == "__main__":
    export_csv()
