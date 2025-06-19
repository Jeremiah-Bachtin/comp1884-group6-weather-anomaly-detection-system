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
    print("⚠️ Warning: VARIABLES in .env is invalid. Using default list.")
    VARIABLES = ["temperature_2m", "surface_pressure", "precipitation", "wind_speed_10m"]

# ===== Open-Meteo API ENDPOINTS ===========

HISTORICAL_API_URL = os.getenv("HISTORICAL_API_URL", "https://archive-api.open-meteo.com/v1/archive")
FORECAST_API_URL = os.getenv("FORECAST_API_URL", "https://forecast-api.open-meteo.com/v1/forecast")

# ===== MODEL CONFIGURATION =====
MODEL_HISTORICAL = os.getenv("MODEL_HISTORICAL", "ecmwf_ifs")
MODEL_FORECAST = os.getenv("MODEL_FORECAST", "ukmo_seamless")

# ===== TIMEZONE CONFIGURATION =====
TIME_ZONE_STRING = os.getenv("TIME_ZONE", "Europe/London")
try:
    TIME_ZONE = pytz.timezone(TIME_ZONE_STRING)
except pytz.UnknownTimeZoneError:
    print(f"⚠️ Invalid timezone in .env: {TIME_ZONE_STRING}. Defaulting to Europe/London.")
    TIME_ZONE = pytz.timezone("Europe/London")

# ===== ANCHOR TIME CONFIGURATION =====
try:
    ANCHOR_TIME_STR = os.getenv("ANCHOR_TIME")  # Get value from .env
    if ANCHOR_TIME_STR:
        ANCHOR_TIME = datetime.fromisoformat(ANCHOR_TIME_STR).astimezone(TIME_ZONE)
    else:
        # Default to now in the specified timezone
        ANCHOR_TIME = datetime.now(TIME_ZONE).replace(minute=0, second=0, microsecond=0)
except Exception as e:
    print(f"⚠️ Invalid ANCHOR_TIME in .env: {e}. Using current time.")
    ANCHOR_TIME = datetime.now(TIME_ZONE).replace(minute=0, second=0, microsecond=0)

# ===== WINDOW CONFIGURATION =====
ROLLING_WINDOW_HOURS = int(os.getenv("ROLLING_WINDOW_HOURS", 1440))
FORECAST_BACKFILL_HOURS = int(os.getenv("FORECAST_BACKFILL_HOURS", 48))
FORECAST_TRIM_HOURS = int(os.getenv("FORECAST_TRIM_HOURS", 72))
FORECAST_PAST_DAYS = int(os.getenv("FORECAST_PAST_DAYS", 3))
FORECAST_FUTURE_DAYS = int(os.getenv("FORECAST_FUTURE_DAYS", 5))
START_YEAR = int(os.getenv("START_YEAR", 2017))
START_MONTH = int(os.getenv("START_MONTH", 1))


