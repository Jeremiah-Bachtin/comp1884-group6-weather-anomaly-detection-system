"""
Merge monthly IFS historical data into a single CSV.
Assumes each monthly CSV has consistent hourly frequency in local timezone.
Includes checks for duplicate timestamps, missing timestamps, and unexpected gaps.
"""

import os
import pandas as pd
from utils.logger import log_event
from utils.find_root import find_project_root

# Setup
PROJECT_ROOT = find_project_root()
HISTORICAL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "historical")
MERGED_DIR = os.path.join(PROJECT_ROOT, "data", "processed", "historical_merged")
os.makedirs(MERGED_DIR, exist_ok=True)

def merge_historical():
    log_event("Started merging historical data.", module="historical_merge")

    files = sorted(f for f in os.listdir(HISTORICAL_DIR) if f.endswith(".csv"))
    all_dfs = []

    total_duplicates = 0
    total_nans = 0
    total_gaps = 0

    for file in files:
        path = os.path.join(HISTORICAL_DIR, file)
        df = pd.read_csv(path, parse_dates=["date"])

        # Check for duplicated timestamps
        duplicated = df.duplicated(subset=["date"]).sum()
        if duplicated > 0:
            log_event(f"NOTICE: {duplicated} duplicated timestamps in {file}. They will be KEPT.", module="historical_merge")
            total_duplicates += duplicated

        # Check for NaNs
        nans = df.isna().sum().sum()
        if nans > 0:
            log_event(f"WARNING: {nans} missing values in {file}.", module="historical_merge")
            total_nans += nans

        # Check for timestamp gaps (should be 1 hour apart)
        df_sorted = df.sort_values("date")
        deltas = df_sorted["date"].diff().dropna()
        gap_violations = (deltas != pd.Timedelta(hours=1)).sum()
        if gap_violations > 0:
            log_event(f"WARNING: {gap_violations} timestamp gaps in {file}.", module="historical_merge")
            total_gaps += gap_violations

        all_dfs.append(df_sorted)

    # Combine all monthly DataFrames and sort chronologically
    merged_df = pd.concat(all_dfs).sort_values("date").reset_index(drop=True)

    start = merged_df["date"].min().strftime("%Y%m")
    end = merged_df["date"].max().strftime("%Y%m")
    output_file = f"historical_IFS_merged_{start}_to_{end}.csv"
    output_path = os.path.join(MERGED_DIR, output_file)
    merged_df.to_csv(output_path, index=False)

    # Final summary
    log_event(f"SUMMARY: Total duplicated timestamps: {total_duplicates}", module="historical_merge")
    log_event(f"SUMMARY: Total missing values (NaNs): {total_nans}", module="historical_merge")
    log_event(f"SUMMARY: Total timestamp gaps: {total_gaps}", module="historical_merge")
    log_event(f"Completed historical merge and saved to {output_path}.", module="historical_merge")

if __name__ == "__main__":
    merge_historical()
