import os
from dotenv import load_dotenv

load_dotenv()

# === Pipeline config ===
LAT = float(os.getenv("LAT", 51.47))
LON = float(os.getenv("LON", -0.4543))
TIMEZONE = os.getenv("TIMEZONE", "Europe/London")

MODEL_FORECAST = os.getenv("MODEL_FORECAST", "ukmo_uk_deterministic_2km")
MODEL_HISTORICAL = os.getenv("MODEL_HISTORICAL", "ecmwf_ifs")

ROLLING_WINDOW_DAYS = int(os.getenv("ROLLING_WINDOW_DAYS", 60))
FORECAST_TRIM_HOURS = int(os.getenv("FORECAST_TRIM_HOURS", 72))

# === Google Drive folder keys ===
DRIVE_FOLDER_DATA = os.getenv("DRIVE_FOLDER_DATA")
DRIVE_FOLDER_PROCESSED = os.getenv("DRIVE_FOLDER_PROCESSED")
DRIVE_FOLDER_ML_INPUT = os.getenv("DRIVE_FOLDER_ML_INPUT")
DRIVE_FOLDER_ROLLING_MEANS = os.getenv("DRIVE_FOLDER_ROLLING_MEANS")
DRIVE_FOLDER_RAW = os.getenv("DRIVE_FOLDER_RAW")
DRIVE_FOLDER_HISTORICAL = os.getenv("DRIVE_FOLDER_HISTORICAL")
DRIVE_FOLDER_FORECAST = os.getenv("DRIVE_FOLDER_FORECAST")

DRIVE_FOLDER_OUTPUTS = os.getenv("DRIVE_FOLDER_OUTPUTS")
DRIVE_FOLDER_COMMUNITY = os.getenv("DRIVE_FOLDER_COMMUNITY")
DRIVE_FOLDER_DASHBOARD_READY = os.getenv("DRIVE_FOLDER_DASHBOARD_READY")
DRIVE_FOLDER_XAI_INPUT = os.getenv("DRIVE_FOLDER_XAI_INPUT")
DRIVE_FOLDER_ANOMALY_FLAGS = os.getenv("DRIVE_FOLDER_ANOMALY_FLAGS")
DRIVE_FOLDER_VISUALISATIONS = os.getenv("DRIVE_FOLDER_VISUALISATIONS")
DRIVE_FOLDER_DASHBOARD_EXPORTS = os.getenv("DRIVE_FOLDER_DASHBOARD_EXPORTS")

# === Reference model names ===
HISTORICAL_MODELS = {
    "ecmwf_ifs": "ECMWF IFS (9km)"
}

FORECAST_MODELS = {
    "ukmo_uk_deterministic_2km": "UKMO UKV (2km)",
    "ukmo_seamless_global": "UKMO Seamless Global (10km)"
}