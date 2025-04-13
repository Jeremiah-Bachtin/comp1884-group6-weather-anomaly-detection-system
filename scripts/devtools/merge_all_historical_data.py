"""
Merge all monthly IFS CSVs into one master CSV
"""

import os
import pandas as pd
from datetime import datetime
import pytz

from scripts.etl.pipeline.utilities.find_root import find_project_root
from scripts.etl.pipeline.utilities.logger import log_event
from config.config import TIMEZONE

# Set up timezone and project root
PROJECT_ROOT = find_project_root()
LONDON_TZ = pytz.timezone(TIMEZONE)

# Define paths
HISTORICAL_DIR = os.path.join(PROJECT_ROOT, "data", "raw", "historical")
MERGED_DIR = os.path.join(HISTORICAL_DIR, "merged")
os.makedirs(MERGED_DIR, exist_ok=True)

def merge_monthly_csvs():
    log_event("Starting monthly IFS merge", module="merge_util")

    files = sorted([f for f in os.listdir(HISTORICAL_DIR)
                    if f.startswith("IFS_") and f.endswith(".csv")])
    if not files:
        log_event("No monthly IFS CSVs found to merge.", module="merge_util")
        return

    all_dfs = []
    for file in files:
        path = os.path.join(HISTORICAL_DIR, file)
        try:
            df = pd.read_csv(path, parse_dates=["date"])
            all_dfs.append(df)
            log_event(f"Merged: {file}", module="merge_util")
        except Exception as e:
            log_event(f"Skipping {file} due to error: {e}", module="merge_util")

    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset="date").sort_values("date")

    # Create human-friendly filename using London time
    local_now = datetime.now(LONDON_TZ).strftime("%Y%m%d_%H")
    filename = f"historical_data_merged_{local_now}.csv"
    output_path = os.path.join(MERGED_DIR, filename)

    merged_df.to_csv(output_path, index=False)

    rel_output_path = os.path.relpath(output_path, PROJECT_ROOT)
    log_event(f"Saved merged IFS data to: {rel_output_path}", module="merge_util")

if __name__ == "__main__":
    merge_monthly_csvs()
