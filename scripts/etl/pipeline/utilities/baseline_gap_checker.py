import os
import pandas as pd
from datetime import datetime
from scripts.etl.pipeline.utilities.find_root import find_project_root

def check_baseline_file(baseline_path):
    print(f"\n🔎 Checking baseline file: {baseline_path}\n" + "-" * 60)

    # Load the baseline dataset
    df = pd.read_csv(baseline_path)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert("Europe/London")
    df = df.sort_values("date")

    # Basic file info
    first_timestamp = df["date"].min()
    last_timestamp = df["date"].max()
    total_rows = len(df)
    total_span_hours = (last_timestamp - first_timestamp) / pd.Timedelta(hours=1)

    print(f"First timestamp: {first_timestamp}")
    print(f"Last timestamp:  {last_timestamp}")
    print(f"Total rows:      {total_rows}")
    print(f"Total span:      {total_span_hours:.1f} hours")

    # Row count check
    expected_rows = 1440
    if abs(total_rows - expected_rows) > 2:
        print(f"\n⚠️ Warning: Rolling window has {total_rows} rows, expected {expected_rows}.")

    # Gap analysis
    print("\n🔍 Gap Analysis:")
    diffs = df["date"].diff()
    gap_counts = diffs.value_counts().sort_index()
    print(gap_counts)

    # Check for missing gaps
    missing_gaps = gap_counts[gap_counts.index > pd.Timedelta(hours=1)]
    if not missing_gaps.empty:
        print("\n❗ Missing intervals detected:")
        print(missing_gaps)
    else:
        print("✅ No missing intervals detected (only normal DST shifts possible).")

    # Extended rows per day analysis
    df["date_only"] = df["date"].dt.floor("D")
    rows_per_day = df.groupby("date_only").size()

    print("\n📊 Rows per day (for reference):")
    print(rows_per_day)

    print(f"\n✅ Days with exactly 24 hours: {sum(rows_per_day == 24)}")
    print(f"⚠️ Days with missing hours (<24h): {sum(rows_per_day < 24)}")
    print(f"⚠️ Days with extra hours (>24h): {sum(rows_per_day > 24)}")

if __name__ == "__main__":
    # Set project root and file location
    project_root = find_project_root()
    baseline_folder = os.path.join(project_root, "data", "processed", "rolling_window")

    # ⚠️ Set your baseline file name here manually each time you check
    filename = "baseline_rolling_1440h_until_20250427_10.csv"  # <<<<< Update accordingly!
    baseline_csv_path = os.path.join(baseline_folder, filename)

    check_baseline_file(baseline_csv_path)
