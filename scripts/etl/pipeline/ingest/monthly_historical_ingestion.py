"""
Monthly ingestion of IFS historical data from Open-Meteo.
Builds monthly CSVs from Jan 2017 to the most recent full month
"""

import os
import calendar
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta, timezone
from scripts.etl.pipeline.utilities.logger import log_event
from scripts.etl.pipeline.utilities.find_root import find_project_root
from config.config import DATA_TZ, LAT, LON, VARIABLES, MODEL_HISTORICAL

# Setup
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.3)
openmeteo = openmeteo_requests.Client(session=retry_session)

PROJECT_ROOT = find_project_root()
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "historical")

# Fetch one month's historical data
def fetch_monthly_ifs(year, month):
    start = datetime(year, month, 1)
    days_in_month = calendar.monthrange(year, month)[1]
    end = start + timedelta(days=days_in_month - 1)

    log_event(f"Fetching historical data for {year}-{month:02d}", module="historical_ingestion")

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "hourly": VARIABLES,
        "models": MODEL_HISTORICAL
    }

    try:
        response = openmeteo.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)[0]
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

        df["date"] = df["date"].dt.tz_convert(DATA_TZ)

        # Optional: re-order columns if needed
        df = df[["date"] + VARIABLES]

        return df

    except Exception as e:
        log_event(f"Failed for {year}-{month:02d}: {e}", module="historical_ingestion")
        return None

# Main ingestion
def run_monthly_ingestion():
    log_event("Started monthly historical ingestion.", module="historical_ingestion")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    start = pd.Timestamp("2017-01-01", tz="UTC")

    while start < cutoff:
        year, month = start.year, start.month
        path = os.path.join(OUTPUT_DIR, f"IFS_{year}_{month:02d}.csv")

        if not os.path.exists(path):
            df = fetch_monthly_ifs(year, month)
            if df is not None:
                df.to_csv(path, index=False)
                log_event(f"Saved monthly data: IFS_{year}_{month:02d}.csv", module="historical_ingestion")
        else:
            log_event(f"Skipped IFS_{year}_{month:02d}.csv – already exists.", module="historical_ingestion")

        start = start + pd.DateOffset(months=1)

    log_event("Completed monthly historical ingestion.", module="historical_ingestion")
