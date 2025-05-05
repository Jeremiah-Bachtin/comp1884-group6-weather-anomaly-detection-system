"""
Hourly ingestion of Open-Meteo forecast + historical data.
Generates a trimmed 72h forecast and a 1440h rolling window up to the anchor time.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from config.config import (
    LAT, LON, VARIABLES, MODEL_FORECAST, MODEL_HISTORICAL,
    ROLLING_WINDOW_HOURS, FORECAST_BACKFILL_HOURS, FORECAST_TRIM_HOURS,
    FORECAST_PAST_DAYS, FORECAST_FUTURE_DAYS, HISTORICAL_API_URL,
    FORECAST_API_URL, TIME_ZONE, ANCHOR_TIME
)
from utils.find_root import find_project_root
from utils.logger import log_event
from utils.fetch_dataframe import fetch_hourly_dataframe
from utils.save_file import save_csv

def fetch_historical_data(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Fetch historical hourly data from Open-Meteo Archive API."""
    log_event(f"Fetching historical data from {start_date.date()} to {end_date.date()}", module="rolling_window_ingestion")

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "hourly": ",".join(VARIABLES),
        "models": MODEL_HISTORICAL,
        "timezone": TIME_ZONE.zone,
    }

    df = fetch_hourly_dataframe(HISTORICAL_API_URL, params)
    if df.empty:
        log_event("Warning: Empty historical dataframe returned.", module="rolling_window_ingestion")
    if df.isna().any().any():
        log_event("Warning: Missing values found in historical data.", module="rolling_window_ingestion")
    return df

def fetch_forecast_data() -> pd.DataFrame:
    """Fetch past and future forecast data from Open-Meteo Forecast API."""
    log_event("Fetching past 3 days + next 5 days forecast", module="forecast_ingestion")

    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ",".join(VARIABLES),
        "models": MODEL_FORECAST,
        "past_days": FORECAST_PAST_DAYS,
        "forecast_days": FORECAST_FUTURE_DAYS,
        "timezone": TIME_ZONE.zone,
    }

    df = fetch_hourly_dataframe(FORECAST_API_URL, params)
    if df.empty:
        log_event("Warning: Empty forecast dataframe returned.", module="forecast_ingestion")
    if df.isna().any().any():
        log_event("Warning: Missing values found in forecast data.", module="forecast_ingestion")
    return df

def save_trimmed_forecast(df: pd.DataFrame, anchor_time: datetime):
    """Save 72-hour trimmed forecast slice."""
    trimmed = df[(df["date"] >= anchor_time) & (df["date"] < anchor_time + timedelta(hours=FORECAST_TRIM_HOURS))].copy()
    fname = f"forecast_72h_from_{anchor_time.strftime('%Y%m%d_%H%M')}.csv"
    save_csv(trimmed, fname, "raw/forecast")
    log_event(f"Saved 72h forecast: {fname} ({len(trimmed)} rows)", module="forecast_ingestion")

def save_rolling_window(df: pd.DataFrame, anchor_time: datetime):
    """Save 1440-hour historical window ending at anchor_time (exclusive)."""
    start = anchor_time - timedelta(hours=ROLLING_WINDOW_HOURS)
    window = df[(df["date"] >= start) & (df["date"] < anchor_time)].copy()

    expected = ROLLING_WINDOW_HOURS
    actual = len(window)
    if abs(actual - expected) > 1:
        log_event(f"Rolling window has {actual} rows, expected {expected}. Δ={actual - expected}", module="rolling_window_ingestion")

    fname = f"baseline_rolling_1440h_until_{(anchor_time - timedelta(hours=1)).strftime('%Y%m%d_%H%M')}.csv"
    save_csv(window, fname, "processed/rolling_window")
    log_event(f"Saved rolling window: {fname} ({len(window)} rows)", module="rolling_window_ingestion")

def main():
    log_event("Starting hourly ingestion anchored at latest full hour.", module="forecast_ingestion")

    anchor = ANCHOR_TIME
    hist_start = anchor - timedelta(hours=ROLLING_WINDOW_HOURS + 1)
    hist_end = anchor - timedelta(hours=FORECAST_BACKFILL_HOURS)

    hist_df = fetch_historical_data(hist_start, hist_end)
    forecast_df = fetch_forecast_data()

    merged_df = pd.concat([hist_df, forecast_df]).drop_duplicates(subset="date").sort_values("date")

    if merged_df.isna().any().any():
        log_event("Warning: NaNs found in merged dataframe.", module="data_integrity")

    save_trimmed_forecast(merged_df, anchor)
    save_rolling_window(merged_df, anchor)

    log_event("Completed hourly ingestion process.", module="forecast_ingestion")

if __name__ == "__main__":
    main()
