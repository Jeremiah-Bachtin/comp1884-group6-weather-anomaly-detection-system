"""
Project-wide logger utility.
Logs timestamped messages into a daily file under data/logs.
"""

import os
from datetime import datetime

# Dynamically set project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(PROJECT_ROOT, "data", "logs")
LOG_FILE = os.path.join(LOG_DIR, "ingestion.log")

def log_event(message, module="general"):
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] [{module}] {message}\n")
    print(f"[LOG - {module}] {message}")

