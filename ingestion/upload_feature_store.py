from utils.hopsworks_conn import get_feature_store

def upload_to_hopsworks(df):
    
    fs = get_feature_store()

    fg = fs.get_or_create_feature_group(
        name="aqi_historical",
        version=1,
        primary_key=["datetime"],
        description="AQI historical dataset"
    )

    fg.insert(df, write_options={"wait_for_job": True})