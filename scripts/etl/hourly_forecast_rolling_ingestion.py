import os
import pandas as pd
from datetime import datetime, timedelta
from config.config import (LAT, LON, VARIABLES, MODEL_FORECAST, MODEL_HISTORICAL,
                            ROLLING_WINDOW_HOURS, FORECAST_BACKFILL_HOURS, FORECAST_TRIM_HOURS,
                           FORECAST_PAST_DAYS, FORECAST_FUTURE_DAYS, HISTORICAL_API_URL,
                           FORECAST_API_URL,DATA_TZ, ANCHOR_TIME)
from utils.find_root import find_project_root
from utils.logger import log_event
from utils.fetch_dataframe import fetch_hourly_dataframe

def fetch_historical_data(start_date, end_date):
    """Fetch historical data slice from Open-Meteo Archive API."""
    log_event(f"Fetching historical data from {start_date.date()} to {end_date.date()}", module="rolling_window_ingestion")

    url = HISTORICAL_API_URL
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": ",".join(VARIABLES),
        "models": MODEL_HISTORICAL,
        "timezone": str(DATA_TZ.zone),
    }

    df = fetch_hourly_dataframe(url, params)
    if df.isna().any().any():
        log_event("Warning: Missing values detected in historical data.", module="rolling_window_ingestion")
    if df.empty:
        log_event("Warning: Empty dataframe returned from Open-Meteo API.", module="rolling_window_ingestion")
    return df

def fetch_forecast_data():
    """
    Fetch forecast data from Open-Meteo.

    Fetches past N days and next M days based on configuration settings.
    """
    log_event("Fetching past 3 days + next 5 days forecast", module="forecast_ingestion")

    url = FORECAST_API_URL
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(VARIABLES),
        "models": MODEL_FORECAST,
        "past_days": FORECAST_PAST_DAYS,
        "forecast_days": FORECAST_FUTURE_DAYS,
        "timezone": str(DATA_TZ.zone),
    }

    df = fetch_hourly_dataframe(url, params)
    if df.isna().any().any():
        log_event("Warning: Missing values detected in forecast data.", module="forecast_ingestion")
    if df.empty:
        log_event("Warning: Empty dataframe returned from Open-Meteo API.", module="forecast_ingestion")
    return df

def save_forecast_csv(forecast_df, anchor_time):
    """Save trimmed 72-hour forecast starting from anchor_time."""
    forecast_slice = forecast_df[
       (forecast_df["date"] >= anchor_time) &
       (forecast_df["date"] < anchor_time + timedelta(hours=FORECAST_TRIM_HOURS))
   ].copy()

    timestamp = anchor_time.strftime("%Y%m%d_%H")
    filename = f"forecast_72h_from_{timestamp}.csv"
    output_dir = os.path.join(find_project_root(), "data", "raw", "forecast")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    forecast_slice.to_csv(output_path, index=False)
    log_event(f"Saved 72h forecast to {filename} with {len(forecast_slice)} rows.", module="forecast_ingestion")

def save_rolling_window(merged_df, anchor_time):
    """Save strict 1440-hour rolling window up to anchor_time - 1."""
    rolling_start = anchor_time - timedelta(hours=ROLLING_WINDOW_HOURS)
    rolling_end = anchor_time

    rolling_df = merged_df[(merged_df["date"] >= rolling_start) & (merged_df["date"] < rolling_end)].copy()

    expected_rows = ROLLING_WINDOW_HOURS
    actual_rows = len(rolling_df)

    if abs(actual_rows - expected_rows) > 1:
        log_event(f"Rolling window has {actual_rows} rows, expected {expected_rows}. There is a difference of {actual_rows-expected_rows}.", module="rolling_window_ingestion")

    timestamp = anchor_time.strftime("%Y%m%d_%H")
    filename = f"baseline_rolling_1440h_until_{timestamp}.csv"
    output_dir = os.path.join(find_project_root(), "data", "processed", "rolling_window")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    rolling_df.to_csv(output_path, index=False)
    log_event(f"Saved rolling window to {filename} with {len(rolling_df)} rows.", module="rolling_window_ingestion")

def main():
    log_event("Starting rolling window generation anchored at the latest full hour.", module="rolling_window_ingestion")

    anchor_time = ANCHOR_TIME

    # Fetch historical slice (60 days back to 2 days ago)
    hist_start = anchor_time - timedelta(hours=ROLLING_WINDOW_HOURS + 1)
    hist_end = anchor_time - timedelta(hours=FORECAST_BACKFILL_HOURS)
    historical_df = fetch_historical_data(hist_start, hist_end)

    # Fetch forecast slice (past 3 days + future 5 days)
    forecast_df = fetch_forecast_data()

    # Merge (historical first, then forecast)
    merged_df = pd.concat([historical_df, forecast_df]).sort_values("date")
    merged_df = merged_df.drop_duplicates(subset="date", keep="first")  # << Prefer historical values

    # Check for missing values in merged data
    if merged_df.isna().any().any():
        log_event("Warning: Missing values found in merged_df (forecast + historical).", module="data_integrity")

    # Save outputs
    save_forecast_csv(merged_df, anchor_time)
    save_rolling_window(merged_df, anchor_time)

    log_event("Completed 72-hour forecast and 1440-hour rolling window ingestion.", module="forecast_ingestion")

if __name__ == "__main__":
    main()