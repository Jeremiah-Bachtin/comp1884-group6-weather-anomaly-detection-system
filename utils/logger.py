import os
from datetime import datetime
import pytz
from utils.find_root import find_project_root

PROJECT_ROOT = find_project_root()
LOG_DIR = os.path.join(PROJECT_ROOT, "data", "logs")
LOG_FILE = os.path.join(LOG_DIR, "ingestion.log")

def log_event(message, module="general"):
    """
    Logs events to a timestamped project log file in /data/logs.

    - Each message is saved with local London time and module name.
    - Also prints the log to console for visibility during execution.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    london = pytz.timezone("Europe/London")
    timestamp = datetime.now(london).strftime("%Y-%m-%d %H:%M:%S %Z")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] [{module}] {message}\n")
    print(f"[LOG - {module}] {message}")
