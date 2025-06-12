import requests
import pandas as pd

def fetch_hourly_dataframe(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "date"}, inplace=True)

    # Since API already returns data in the requested timezone, just parse as-is
    df["date"] = pd.to_datetime(df["date"])
    
    df.drop_duplicates(subset="date", keep="first", inplace=True)
    return df