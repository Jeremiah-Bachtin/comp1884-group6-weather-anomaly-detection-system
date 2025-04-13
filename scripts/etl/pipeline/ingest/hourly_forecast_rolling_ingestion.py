import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from scripts.etl.pipeline.utilities.logger import log_event

# === Find project root ===
def find_project_root():
    current = os.path.abspath(__file__)
    while not os.path.exists(os.path.join(current, 'README.md')):
        current = os.path.dirname(current)
        if current == os.path.dirname(current):  # Reached root
            raise FileNotFoundError("Could not locate project root (missing README.md)")
    return current

PROJECT_ROOT = find_project_root()

# === Config ===
LAT, LON = 51.47, -0.4543
VARIABLES = ["temperature_2m", "surface_pressure", "precipitation", "wind_speed_10m"]
MODEL = "ukmo_seamless"
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
FORECAST_DIR = os.path.join(DATA_DIR, "forecast")
ROLLING_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "rolling_means")

# === API Setup ===
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.3)
openmeteo = openmeteo_requests.Client(session=retry_session)

# === Ingestion Logic ===
def fetch_historical_window():
    end = (datetime.now(timezone.utc) - timedelta(days=2)).date()
    start = end - timedelta(days=60)
    log_event("Fetching 60-day historical window", module="hourly_ingestion")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "hourly": VARIABLES,
        "models": "ecmwf_ifs"
    }

    response = openmeteo.weather_api(url, params=params)[0]
    hourly = response.Hourly()
    return pd.DataFrame({
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        **{var: hourly.Variables(i).ValuesAsNumpy() for i, var in enumerate(VARIABLES)}
    })

def fetch_forecast_prediction():
    log_event("Fetching 3-day forecast", module="hourly_ingestion")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": VARIABLES,
        "models": MODEL,
        "forecast_days": 5
    }

    response = openmeteo.weather_api(url, params=params)[0]
    hourly = response.Hourly()
    df = pd.DataFrame({
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        **{var: hourly.Variables(i).ValuesAsNumpy() for i, var in enumerate(VARIABLES)}
    })

    now = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
    return df[(df["date"] >= now) & (df["date"] < now + pd.Timedelta(hours=72))]

def save_to_csv(df, folder, prefix):
    os.makedirs(folder, exist_ok=True)
    df_clean = df.dropna()
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    filename = f"{prefix}_{now}.csv"
    path = os.path.join(folder, filename)
    df_clean.to_csv(path, index=False)
    log_event(f"Saved {prefix} CSV: {filename}", module="hourly_ingestion")

def run_ingestion():
    log_event("Starting hourly ingestion", module="hourly_ingestion")
    historical_df = fetch_historical_window()
    forecast_df = fetch_forecast_prediction()

    stitched_df = pd.concat([historical_df, forecast_df])
    stitched_df = stitched_df.drop_duplicates(subset="date").sort_values("date")

    save_to_csv(stitched_df, ROLLING_DIR, "baseline")
    save_to_csv(forecast_df, FORECAST_DIR, "forecast")
    log_event("Hourly ingestion complete", module="hourly_ingestion")

if __name__ == "__main__":
    run_ingestion()
