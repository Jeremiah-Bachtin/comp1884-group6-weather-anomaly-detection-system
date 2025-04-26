import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from config.config import (LAT, LON, VARIABLES, MODEL_FORECAST, MODEL_FORECAST_BACKUP,
                           FORECAST_TRIM_HOURS, FORECAST_BACKFILL_HOURS, ROLLING_WINDOW_DAYS, DATA_TZ)
from scripts.etl.pipeline.utilities.find_root import find_project_root
from scripts.etl.pipeline.utilities.logger import log_event
from scripts.etl.pipeline.utilities.fetch_dataframe import fetch_hourly_dataframe


def fetch_combined_forecast():
    log_event("Requesting combined forecast (backfill + forward)", module="forecast_ingestion")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(VARIABLES),
        "models": f"{MODEL_FORECAST},{MODEL_FORECAST_BACKUP}",
        "forecast_hours": FORECAST_TRIM_HOURS,
        "past_days": FORECAST_BACKFILL_HOURS // 24,
        "timezone": str(DATA_TZ.zone)
    }

    df = fetch_hourly_dataframe(url, params)

    now = datetime.now(DATA_TZ).replace(minute=0, second=0, microsecond=0)

    # Split forecast into two parts: backfill (past 48h) and forecast (next 72h)
    backfill_start = now - timedelta(hours=FORECAST_BACKFILL_HOURS)
    backfill_end = now
    forecast_end = now + timedelta(hours=FORECAST_TRIM_HOURS)

    backfill_df = df[(df["date"] >= backfill_start) & (df["date"] < backfill_end)].copy()
    forecast_df = df[(df["date"] >= now) & (df["date"] < forecast_end)].copy()

    # Validate forecast_df length
    expected_forecast_rows = FORECAST_TRIM_HOURS
    if len(forecast_df) != expected_forecast_rows:
        log_event(f"Warning: forecast_df has {len(forecast_df)} rows, expected {expected_forecast_rows}.", module="forecast_ingestion")

    return backfill_df, forecast_df


def save_forecast_csv(df):
    timestamp = datetime.now(DATA_TZ).strftime("%Y%m%d_%H")
    filename = f"forecast_{timestamp}.csv"
    output_dir = os.path.join(find_project_root(), "data", "raw", "forecast")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    if os.path.exists(output_path):
        log_event(f"{filename} already exists, skipping save.", module="forecast_ingestion")
    else:
        df.to_csv(output_path, index=False)
        log_event(f"Saved {filename} with {len(df)} rows.", module="forecast_ingestion")


def load_historical_baseline():
    log_event("Requesting 58-day historical data from Open-Meteo API", module="forecast_ingestion")

    url = "https://archive-api.open-meteo.com/v1/archive"
    now = datetime.now(DATA_TZ).replace(minute=0, second=0, microsecond=0)
    start_date = (now - timedelta(days=ROLLING_WINDOW_DAYS)).strftime("%Y-%m-%d")
    end_date = (now - timedelta(hours=FORECAST_BACKFILL_HOURS)).strftime("%Y-%m-%d")

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(VARIABLES),
        "models": MODEL_FORECAST,
        "timezone": str(DATA_TZ.zone)
    }

    df = fetch_hourly_dataframe(url, params)

    return df

def save_rolling_window(df):
    # Check rolling window validity
    actual_start = df["date"].min()
    actual_end = df["date"].max()
    duration_hours = (actual_end - actual_start) / pd.Timedelta(hours=1)

    expected_hours = (ROLLING_WINDOW_DAYS - 2) * 24 + FORECAST_BACKFILL_HOURS

    if abs(duration_hours - expected_hours) > 1:
        log_event(f"Warning: rolling window spans {duration_hours:.1f} hours, expected {expected_hours}.", module="forecast_ingestion")

    duplicates = df.duplicated(subset="date").sum()
    if duplicates > 0:
        log_event(f"Warning: rolling window contains {duplicates} duplicated timestamps.", module="forecast_ingestion")

    timestamp = datetime.now(DATA_TZ).strftime("%Y%m%d_%H")
    filename = f"baseline_{timestamp}.csv"
    output_dir = os.path.join(find_project_root(), "data", "processed", "rolling_window")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    df.to_csv(output_path, index=False)
    log_event(f"Saved rolling window to {filename} with {len(df)} rows.", module="forecast_ingestion")


def main():
    log_event("Starting forecast ingestion and rolling window generation", module="forecast_ingestion")

    backfill_df, forecast_df = fetch_combined_forecast()
    save_forecast_csv(forecast_df)

    historical_df = load_historical_baseline()
    rolling_window_df = pd.concat([historical_df, backfill_df], ignore_index=True)
    save_rolling_window(rolling_window_df)

    log_event("Forecast ingestion and rolling window complete", module="forecast_ingestion")


if __name__ == "__main__":
    main()
