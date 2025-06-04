# main.py

try:
    from config.original_config import VARIABLES
except ImportError:
    from config.config import VARIABLES

print("Environment ready. VARIABLES =", VARIABLES)
