import os
import ast
from dotenv import load_dotenv
import pytz
from datetime import datetime, timedelta

# Load variables from .env
load_dotenv()

# ===== LOCATION & VARIABLE SETTINGS =====
LAT = float(os.getenv("LAT", 51.47))
LON = float(os.getenv("LON", -0.4543))

# Parse variable list safely from string
try:
    VARIABLES = ast.literal_eval(os.getenv("VARIABLES", '["temperature_2m", "surface_pressure", "precipitation", "wind_speed_10m"]'))
    if not isinstance(VARIABLES, list):
        raise ValueError
except (SyntaxError, ValueError):
    raise ValueError("VARIABLES in .env must be a valid Python list format.")

# ===== Open-Meteo API ENDPOINTS ===========

HISTORICAL_API_URL = os.getenv("HISTORICAL_API_URL", "https://archive-api.open-meteo.com/v1/archive")
FORECAST_API_URL = os.getenv("FORECAST_API_URL", "https://forecast-api.open-meteo.com/v1/forecast")

# ===== MODEL CONFIGURATION =====
MODEL_HISTORICAL = os.getenv("MODEL_HISTORICAL", "ecmwf_ifs")
MODEL_FORECAST = os.getenv("MODEL_FORECAST", "ukmo_seamless")

# ===== TIMEZONE HANDLING =====
DATA_TZ_STRING = os.getenv("DATA_TIMESTAMP_TZ", "Europe/London")
LOG_TZ_STRING = os.getenv("LOG_TIMESTAMP_TZ", "Europe/London")
try:
    DATA_TZ = pytz.timezone(DATA_TZ_STRING)
    LOG_TZ = pytz.timezone(LOG_TZ_STRING)
except pytz.UnknownTimeZoneError:
    raise ValueError(f"Invalid timezone in .env: {DATA_TZ_STRING} or {LOG_TZ_STRING}")

# ===== ANCHOR TIME CONFIGURATION =====
try:
    ANCHOR_TIME_STR = os.getenv("ANCHOR_TIME")  # Get value from .env
    if ANCHOR_TIME_STR:
        ANCHOR_TIME = datetime.fromisoformat(ANCHOR_TIME_STR).astimezone(DATA_TZ)
    else:
        # Default to now - 1 hour in the specified timezone
        ANCHOR_TIME = (datetime.now(DATA_TZ) - timedelta(hours=1))
except ValueError:
    raise ValueError("Invalid ANCHOR_TIME format in .env. Use ISO format: 'YYYY-MM-DDTHH:MM:SS'.")

# ===== WINDOW CONFIGURATION =====
ROLLING_WINDOW_HOURS = int(os.getenv("ROLLING_WINDOW_HOURS", 1440))
FORECAST_BACKFILL_HOURS = int(os.getenv("FORECAST_BACKFILL_HOURS", 48))
FORECAST_TRIM_HOURS = int(os.getenv("FORECAST_TRIM_HOURS", 72))
FORECAST_PAST_DAYS = int(os.getenv("FORECAST_PAST_DAYS", 3))
FORECAST_FUTURE_DAYS = int(os.getenv("FORECAST_FUTURE_DAYS", 5))

# ===== GOOGLE DRIVE FOLDER IDs =====
DRIVE_FOLDER_DATA = os.getenv("DRIVE_FOLDER_DATA")
DRIVE_FOLDER_PROCESSED = os.getenv("DRIVE_FOLDER_PROCESSED")
DRIVE_FOLDER_ML_INPUT = os.getenv("DRIVE_FOLDER_ML_INPUT")
DRIVE_FOLDER_ROLLING_WINDOW = os.getenv("DRIVE_FOLDER_ROLLING_WINDOW")
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

# ===== CREDENTIALS =====
CLIENT_SECRETS_PATH = os.getenv("CLIENT_SECRETS_PATH", "client_secrets.json")
