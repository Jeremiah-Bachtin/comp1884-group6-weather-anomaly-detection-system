"""
Monthly ingestion of IFS historical data from Open-Meteo
Builds monthly CSVs from Jan 2017 to two days before current date
"""

import os
import calendar
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta, timezone
from scripts.etl.pipeline.utilities.logger import log_event

# API setup
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.3)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Configuration
LAT, LON = 51.47, -0.4543
VARIABLES = ["temperature_2m", "surface_pressure", "precipitation", "wind_speed_10m"] # , "wind_gusts_10m"
MODEL = "ecmwf_ifs"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # /Data_Ingestion_Pipeline
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(DATA_DIR, "historical", "training")

def fetch_monthly_ifs(year, month):
    start_date = datetime(year, month, 1)
    end_date = start_date + timedelta(days=calendar.monthrange(year, month)[1] - 1)
    log_event(f"Requesting IFS data for {year}-{month:02d}", module="training")

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": VARIABLES,
        "models": MODEL
    }

    try:
        responses = openmeteo.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)
        response = responses[0]
        hourly = response.Hourly()
        data = {
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
        }
        return pd.DataFrame(data)
    except Exception as e:
        log_event(f"Failed to fetch data for {year}-{month:02d}: {e}", module="training")
        return None

def save_month_to_csv(df, year, month):
    filename = f"IFS_{year}_{month:02d}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(path, index=False)
    log_event(f"Saved monthly data: {filename}", module="training")

def run_monthly_ingestion():
    log_event("Started monthly IFS historical ingestion process.", module="training")
    cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    start = pd.Timestamp("2017-01-01", tz="UTC")

    while start < cutoff:
        year, month = start.year, start.month
        filename = f"IFS_{year}_{month:02d}.csv"
        path = os.path.join(OUTPUT_DIR, filename)

        if not os.path.exists(path):
            df = fetch_monthly_ifs(year, month)
            if df is not None:
                save_month_to_csv(df, year, month)
        else:
            log_event(f"Skipped {filename} – already exists.", module="training")

        if month == 12:
            start = pd.Timestamp(f"{year + 1}-01-01", tz="UTC")
        else:
            start = pd.Timestamp(f"{year}-{month + 1:02d}-01", tz="UTC")

    log_event("Completed monthly historical ingestion.", module="training")

if __name__ == "__main__":
    run_monthly_ingestion()
