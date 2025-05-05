import requests
import pandas as pd
from config.config import TIME_ZONE

def fetch_hourly_dataframe(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "date"}, inplace=True)

    # Localise using TIME_ZONE
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(TIME_ZONE, nonexistent="shift_forward")

    df.drop_duplicates(subset="date", keep="first", inplace=True)
    return df
