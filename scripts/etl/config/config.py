"""
Centralised config using dotenv
"""

import os
from dotenv import load_dotenv

load_dotenv()

LAT = float(os.getenv("LATITUDE", 51.47))
LON = float(os.getenv("LONGITUDE", -0.4543))
TIMEZONE = os.getenv("TIMEZONE", "Europe/London")
MODEL_FORECAST = os.getenv("MODEL_FORECAST", "ukmo_uk_deterministic_2km")
MODEL_HISTORICAL = os.getenv("MODEL_HISTORICAL", "ecmwf_ifs")
