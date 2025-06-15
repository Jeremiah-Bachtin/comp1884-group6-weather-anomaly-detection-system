import requests
import pandas as pd

def fetch_hourly_dataframe(url, params):
    """
    Fetches hourly weather data from the Open-Meteo API and returns it as a cleaned DataFrame.

    - Parses JSON API response into a Pandas DataFrame.
    - Renames time column to 'date' and parses as datetime.
    - Removes any duplicate timestamps.
    """

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "date"}, inplace=True)

    # Since API already returns data in the requested timezone, just parse as-is
    df["date"] = pd.to_datetime(df["date"])

    # Drop duplicate hourly records if any exist
    df.drop_duplicates(subset="date", keep="first", inplace=True)
    return df