import requests
import pandas as pd
from config.config import DATA_TZ

def fetch_hourly_dataframe(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(DATA_TZ, nonexistent='shift_forward')
    df = df.drop_duplicates(subset="date", keep="first")
    return df
