"""
Merge monthly IFS historical data into a single CSV.
Includes checks for missing and duplicated timestamps,
accounting for DST effects.
"""

import os
import pandas as pd
from utils.logger import log_event
from utils.find_root import find_project_root
from config.config import DATA_TZ

# Setup
PROJECT_ROOT = find_project_root()
HISTORICAL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "historical")
MERGED_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "merged_historical.csv")

def merge_historical():
    log_event("Started merging historical data.", module="historical_merge")

    # Ensure the output directory exists
    project_root = find_project_root()
    output_dir = os.path.join(project_root, "data", "processed", "historical_merged")
    os.makedirs(output_dir, exist_ok=True)

    files = sorted([f for f in os.listdir(HISTORICAL_DIR) if f.endswith(".csv")])
    all_dfs = []

    # Counters
    total_duplicates = 0
    total_missing = 0
    total_nans = 0

    for file in files:
        path = os.path.join(HISTORICAL_DIR, file)
        df = pd.read_csv(path, parse_dates=["date"])
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(DATA_TZ)

        # Check for duplicated timestamps
        duplicated = df.duplicated(subset=["date"]).sum()
        if duplicated > 0:
            log_event(
                f"NOTICE: {duplicated} duplicated timestamps found in {file} (likely October DST). They will be KEPT.",
                module="historical_merge"
            )
            total_duplicates += duplicated

        # Check for missing timestamps (based on expected range)
        expected_hours = int((df["date"].max() - df["date"].min()) / pd.Timedelta(hours=1)) + 1
        actual_hours = df["date"].shape[0]
        if expected_hours != actual_hours:
            missing = expected_hours - actual_hours
            log_event(
                f"NOTICE: {missing} missing timestamps detected in {file} (likely March DST).",
                module="historical_merge"
            )
            total_missing += missing

        # Check for NaNs
        if df.isna().any().any():
            count_nans = df.isna().sum().sum()
            log_event(
                f"WARNING: {count_nans} missing values detected in {file}.",
                module="historical_merge"
            )
            total_nans += count_nans

        all_dfs.append(df)

    # Merge all the dataframes into one
    merged_df = pd.concat(all_dfs).sort_values("date").reset_index(drop=True)

    # Determine the dynamic file name
    start_date = merged_df["date"].min()
    end_date = merged_df["date"].max()
    start_yearmonth = start_date.strftime("%Y%m")
    end_yearmonth = end_date.strftime("%Y%m")
    output_filename = f"historical_IFS_merged_{start_yearmonth}_to_{end_yearmonth}.csv"

    # Save merged dataset
    output_path = os.path.join(output_dir, output_filename)
    merged_df.to_csv(output_path, index=False)

    # Final DST anomaly summary
    log_event(f"SUMMARY: Total duplicated timestamps across all files: {total_duplicates}", module="historical_merge")
    log_event(f"SUMMARY: Total missing timestamps across all files: {total_missing}", module="historical_merge")
    log_event(f"SUMMARY: Total missing values (NaNs) across all files: {total_nans}", module="historical_merge")
    log_event(f"Completed historical merge and saved to {output_path}.", module="historical_merge")

if __name__ == "__main__":
    merge_historical()
