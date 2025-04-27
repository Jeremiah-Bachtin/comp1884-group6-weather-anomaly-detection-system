import os

def find_project_root():
    """Walk up directories until 'requirements.txt' is found."""
    current = os.path.abspath(__file__)
    while True:
        parent = os.path.dirname(current)
        if os.path.exists(os.path.join(parent, "requirements.txt")):
            return parent
        if parent == current:
            raise FileNotFoundError("Could not locate project root (missing requirements.txt)")
        current = parent