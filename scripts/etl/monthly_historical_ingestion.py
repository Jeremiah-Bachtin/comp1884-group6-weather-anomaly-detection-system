"""
Monthly ingestion of IFS historical data from Open-Meteo.
Builds monthly CSVs from Feb 2017 to the most recent *full* month.
"""

import os
import calendar
import pandas as pd
import requests
from datetime import datetime, timedelta
from config.original_config import (
    LAT, LON, VARIABLES, TIME_ZONE,
    MODEL_HISTORICAL, HISTORICAL_API_URL,
    START_YEAR, START_MONTH
)
from utils.find_root import find_project_root
from utils.logger import log_event
from utils.save_file import save_csv

# Compute cutoff: API lags by 2 days, so exclude current or partial month
today_utc = datetime.utcnow().date()
latest_safe_date = today_utc - timedelta(days=2)
latest_full_month = datetime(latest_safe_date.year, latest_safe_date.month, 1).date()

def fetch_month(year: int, month: int):
    """Fetch a month's historical data using Open-Meteo REST API and save as CSV."""
    start_date = datetime(year, month, 1).date()
    end_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, end_day).date()

    # Skip partial/incomplete months
    if end_date >= latest_safe_date:
        return

    filename = f"IFS_{year}_{month:02d}.csv"
    output_dir = os.path.join(find_project_root(), "data", "raw", "historical")
    file_path = os.path.join(output_dir, filename)

    if os.path.exists(file_path):
        log_event(f"Skipped {filename} – already exists.", module="historical_ingestion")
        return

    log_event(f"Fetching historical data for {year}-{month:02d}", module="historical_ingestion")

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(VARIABLES),
        "models": MODEL_HISTORICAL,
        "timezone": TIME_ZONE.zone  # API returns timestamps in this timezone
    }

    try:
        response = requests.get(HISTORICAL_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "hourly" not in data or "time" not in data["hourly"]:
            raise ValueError("Malformed API response – missing 'hourly' or 'time'.")

        df = pd.DataFrame(data["hourly"])
        df.rename(columns={"time": "date"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"]) #.dt.tz_localize(TIME_ZONE.zone, nonexistent="shift_forward") to localise to London time

        # Validate expected number of rows
        expected_rows = calendar.monthrange(year, month)[1] * 24
        if len(df) < expected_rows:
            log_event(
                f"Warning: {filename} contains {len(df)} rows, expected {expected_rows}. Skipping save.",
                module="historical_ingestion"
            )
            return

        save_csv(df, filename, "raw/historical")
        log_event(f"Saved monthly data: {filename}", module="historical_ingestion")

    except Exception as e:
        log_event(f"Failed to fetch data for {year}-{month:02d}: {e}", module="historical_ingestion")

def run_monthly_ingestion():
    log_event("Started monthly historical ingestion.", module="historical_ingestion")

    current = datetime(START_YEAR, START_MONTH, 1).date()
    while current < latest_full_month:
        fetch_month(current.year, current.month)
        # Advance to next month
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1).date()
        else:
            current = datetime(current.year, current.month + 1, 1).date()

    log_event("Completed monthly historical ingestion.", module="historical_ingestion")

if __name__ == "__main__":
    run_monthly_ingestion()