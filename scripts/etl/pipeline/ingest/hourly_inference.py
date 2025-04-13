"""
Hourly inference ingestion pipeline:
1. Loads 60-day historical rolling window (T-62 to T-2)
2. Fetches 2-day backfill via forecast API (past_days=2)
3. Fetches 3-day forecast for anomaly detection
4. Stitches historical + backfill to create rolling baseline
5. Saves forecast and rolling baseline to CSV
"""

import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from scripts.etl.pipeline.utilities.logger import log_event

# API setup
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.3)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Config
LAT, LON = 51.47, -0.4543
VARIABLES = ["temperature_2m", "surface_pressure","precipitation", "wind_speed_10m"] #"wind_gusts_10m",
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODEL = "ukmo_seamless" #or switch to 2-km resolution "ukmo_uk_deterministic_2km"
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
FORECAST_DIR = os.path.join(DATA_DIR, "forecast")
ROLLING_DIR = os.path.join(DATA_DIR, "rolling_baseline")

def fetch_historical_window():
    end = (datetime.now(timezone.utc) - timedelta(days=2)).date()
    start = (end - timedelta(days=60))

    log_event("Fetching 60-day historical window", module="inference")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "hourly": VARIABLES,
        "models": "ecmwf_ifs"
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    df = pd.DataFrame({
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        VARIABLES[0]: hourly.Variables(0).ValuesAsNumpy(),
        VARIABLES[1]: hourly.Variables(1).ValuesAsNumpy(),
        VARIABLES[2]: hourly.Variables(2).ValuesAsNumpy(),
        VARIABLES[3]: hourly.Variables(3).ValuesAsNumpy(),
        # VARIABLES[4]: hourly.Variables(4).ValuesAsNumpy()
    })

    return df

def fetch_forecast_backfill():
    log_event("Fetching 2-day forecast backfill", module="inference")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": VARIABLES,
        "models": MODEL,
        "past_days": 2,
        "forecast_days": 0
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
        VARIABLES[0]: hourly.Variables(0).ValuesAsNumpy(),
        VARIABLES[1]: hourly.Variables(1).ValuesAsNumpy(),
        VARIABLES[2]: hourly.Variables(2).ValuesAsNumpy(),
        VARIABLES[3]: hourly.Variables(3).ValuesAsNumpy(),
        # VARIABLES[4]: hourly.Variables(4).ValuesAsNumpy()
    })


def fetch_forecast_prediction():
    log_event("Fetching 3-day forecast prediction (5-day buffer)", module="inference")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": VARIABLES,
        "models": MODEL,
        "past_days": 0,
        "forecast_days": 5  # buffer to ensure full coverage
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
        VARIABLES[0]: hourly.Variables(0).ValuesAsNumpy(),
        VARIABLES[1]: hourly.Variables(1).ValuesAsNumpy(),
        VARIABLES[2]: hourly.Variables(2).ValuesAsNumpy(),
        VARIABLES[3]: hourly.Variables(3).ValuesAsNumpy(),
        # VARIABLES[4]: hourly.Variables(4).ValuesAsNumpy()
    })

    # Trim to exactly the next 72 hours from now
    now = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
    df = df[(df["date"] >= now) & (df["date"] < now + pd.Timedelta(hours=72))]

    return df

def save_to_csv(df, folder, prefix):
    os.makedirs(folder, exist_ok=True)
    df_clean = df.dropna()
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    filename = f"{prefix}_{now}.csv"
    path = os.path.join(folder, filename)
    df_clean.to_csv(path, index=False)
    log_event(f"Saved {prefix} CSV: {filename}", module="inference")

def run_hourly_ingestion():
    log_event("Starting hourly inference ingestion", module="inference")
    hist_df = fetch_historical_window()
    backfill_df = fetch_forecast_backfill()
    stitched_df = pd.concat([hist_df, backfill_df]).drop_duplicates(subset="date").sort_values("date")
    forecast_df = fetch_forecast_prediction()
    save_to_csv(stitched_df, ROLLING_DIR, "baseline")
    save_to_csv(forecast_df, FORECAST_DIR, "forecast")
    log_event("Hourly ingestion process completed.", module="inference")

if __name__ == "__main__":
    run_hourly_ingestion()
