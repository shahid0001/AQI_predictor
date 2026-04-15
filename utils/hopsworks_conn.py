import os
import hopsworks
from dotenv import load_dotenv

load_dotenv()


def get_feature_store():
    project = hopsworks.login(
        project="air_quality_prediction",
        api_key_value=os.getenv("HOPSWORKS_KEY")
    )

    fs = project.get_feature_store()
    print("Connected to Hopsworks Feature Store ")
    return fs