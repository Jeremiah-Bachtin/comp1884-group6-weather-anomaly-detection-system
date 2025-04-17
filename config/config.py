import os
import ast
from dotenv import load_dotenv
import pytz

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

# ===== MODEL CONFIGURATION =====
MODEL_HISTORICAL = os.getenv("MODEL_HISTORICAL", "ecmwf_ifs")
MODEL_FORECAST = os.getenv("MODEL_FORECAST", "ukmo_uk_deterministic_2km")
MODEL_FORECAST_BACKUP = os.getenv("MODEL_FORECAST_BACKUP", "ukmo_seamless")

# ===== TIMEZONE HANDLING =====
DATA_TZ_STRING = os.getenv("DATA_TIMESTAMP_TZ", "Europe/London")
LOG_TZ_STRING = os.getenv("LOG_TIMESTAMP_TZ", "Europe/London")
try:
    DATA_TZ = pytz.timezone(DATA_TZ_STRING)
    LOG_TZ = pytz.timezone(LOG_TZ_STRING)
except pytz.UnknownTimeZoneError:
    raise ValueError(f"Invalid timezone in .env: {DATA_TZ_STRING} or {LOG_TZ_STRING}")

# ===== WINDOW CONFIGURATION =====
ROLLING_WINDOW_DAYS = int(os.getenv("ROLLING_WINDOW_DAYS", 60))
FORECAST_BACKFILL_HOURS = int(os.getenv("FORECAST_BACKFILL_HOURS", 48))
FORECAST_TRIM_HOURS = int(os.getenv("FORECAST_TRIM_HOURS", 72))

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
