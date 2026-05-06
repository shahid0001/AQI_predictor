import pandas as pd

def clean_data(df):
    # Convert datetime
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Sort
    df = df.sort_values('datetime')

    # Remove duplicates
    df = df.drop_duplicates(subset='datetime')

    # Handle missing values
    df = df.fillna(method='ffill')

    return df