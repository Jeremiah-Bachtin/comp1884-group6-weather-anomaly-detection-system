# COMP1884 - Group 6 - Data Ingestion Scripts Documentation

## 1. Hourly Forecast Rolling Ingestion Documentation

### **Purpose**
Creates two main outputs every hour:
1. **72-hour forecast** (next 3 days from now)
2. **1440-hour rolling window** (past 60 days of historical data)

#### **How It Works**

#### **Data Sources**
- **Historical**: ECMWF IFS model via Open-Meteo Archive API
- **Forecast**: UKMO Seamless model via Open-Meteo Forecast API

#### **Processing Steps**
1. **Calculate anchor time** (current hour, timezone-naive)
2. **Fetch historical data** (1441 hours before anchor to 48 hours before)
3. **Fetch forecast data** (3 days past + 5 days future)
4. **Merge and deduplicate** all data by timestamp
5. **Save two outputs**:
   - Trimmed 72h forecast from anchor time
   - Rolling 1440h window ending at anchor time

#### **File Outputs**
```
data/raw/forecast/forecast_72h_{most recent current hour}.csv
data/processed/rolling_window/baseline_rolling_1440h_until_{one hour before the most recent complete hour}.csv
```


#### ⚙️ **Configuration**
- **Rolling window**: 1440 hours (60 days)
- **Forecast trim**: 72 hours (3 days)
- **Backfill overlap**: 48 hours (prevents gaps)
- **Location**: Heathrow (51.47°N, -0.4543°W)
- **Variables**: temperature_2m, surface_pressure, precipitation, wind_speed_10m

#### **Data Quality Checks**
- Empty dataframe detection
- Missing value warnings
- Row count validation (expected: 1440 rows)
- Overlap handling via deduplication

#### **Top 3 Improvement Suggestions**

- **Add data quality metrics**: Track missing value percentages and flag extreme weather values outside realistic ranges
- **Implement API retry logic**: Auto-retry failed API calls with exponential backoff to handle temporary network issues
- **Add incremental processing**: Check existing files to avoid re-fetching overlapping historical data unnecessarily

--- 

## 2. Monthly Historical Ingestion Script 
### What the Script Does
The **Monthly Historical Ingestion Script** is a **weather data collector** that automatically downloads historical weather information from the internet and organizes it on your computer.
### Step-by-Step Process:
1. **Gets Weather Data from the Internet**
    - Connects to a weather service called "Open-Meteo"
    - Requests weather data from a specific location (Heathrow area coordinates)
    - Downloads hourly data for temperature, air pressure, rainfall, and wind speed

2. **Downloads Data Month by Month**
    - Retrieves one month at a time instead of all data at once (which would be huge)
    - Starts from February 2017 and works up to the most recent complete month
    - Saves each month as a separate file with names like `IFS_2017_02.csv`

3. **Checks Data Quality**
    - Validates each month's data before saving
    - Counts hours of data received (should be 24 hours × days in month)
    - Writes warning messages and skips saving if data looks incorrect

4. **Avoids Duplicate Work**
    - Skips downloading if a monthly file already exists
    - Saves time and internet bandwidth on subsequent runs

5. **Keeps a Log**
    - Records all activities in a log file
    - Helps track progress and troubleshoot issues

### What Gets Saved:
- Monthly CSV files in `data/raw/historical/` folder
- Hourly weather measurements for each month
- Timestamps and weather values (temperature, rainfall, etc.)

### Top 3 Improvement Suggestions

- **Add API retry logic**: Auto-retry failed calls 2-3 times with exponential backoff for network resilience 
- **Implement data quality checks**: Validate weather values within realistic ranges and track missing data percentages
- **Add incremental processing**: Skip re-fetching overlapping data by checking existing files and resuming from interruption points
