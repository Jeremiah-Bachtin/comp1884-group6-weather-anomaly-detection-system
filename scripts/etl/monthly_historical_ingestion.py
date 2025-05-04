"""
Monthly ingestion of IFS historical data from Open-Meteo.
Builds monthly CSVs from Jan 2017 to the most recent full month.
"""

import os
import calendar
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta, timezone
from utils.logger import log_event
from utils.find_root import find_project_root
from config.config import (DATA_TZ, LAT, LON, VARIABLES, MODEL_HISTORICAL,
                           HISTORICAL_API_URL, START_YEAR, START_MONTH)

# Setup
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.3)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Config
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
        response = openmeteo.weather_api(HISTORICAL_API_URL, params=params)[0]
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

        # Strictly keep timestamps inside the month
        month_start = pd.Timestamp(year, month, 1, tz="UTC").tz_convert(DATA_TZ)
        month_end = (month_start + pd.DateOffset(months=1))
        df = df[(df["date"] >= month_start) & (df["date"] < month_end)]

        # Optional: re-order columns
        df = df[["date"] + VARIABLES]

        # Check for missing values
        if df.isna().any().any():
            log_event(f"Warning: Missing values detected in IFS data for {year}-{month:02d}.", module="historical_ingestion")

        return df

    except Exception as e:
        log_event(f"Failed for {year}-{month:02d}: {e}", module="historical_ingestion")
        return None

# Main ingestion
def run_monthly_ingestion():
    log_event("Started monthly historical ingestion.", module="historical_ingestion")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=2)
    start = pd.Timestamp(f"{START_YEAR}-{START_MONTH:02d}-01", tz="UTC")

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

if __name__ == "__main__":
    run_monthly_ingestion()
