"""
Project-wide logger utility.
Logs timestamped messages into a daily file under logs/.
"""

import os
from datetime import datetime
import pytz
from scripts.etl.pipeline.utilities.find_root import find_project_root

PROJECT_ROOT = find_project_root()
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
LOG_FILE = os.path.join(LOG_DIR, "ingestion.log")

def log_event(message, module="general"):
    os.makedirs(LOG_DIR, exist_ok=True)
    london = pytz.timezone("Europe/London")
    timestamp = datetime.now(london).strftime("%Y-%m-%d %H:%M:%S %Z")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] [{module}] {message}\n")
    print(f"[LOG - {module}] {message}")
