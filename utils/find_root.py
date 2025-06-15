import os

def find_project_root():
    """
    Finds the root directory of the project by walking up until 'requirements.txt' is found.

    - Helps ensure paths are always relative to the project root, regardless of where the script is run from.
    """

    current = os.path.abspath(__file__)
    while True:
        parent = os.path.dirname(current)
        if os.path.exists(os.path.join(parent, "requirements.txt")):
            return parent
        if parent == current:
            raise FileNotFoundError("Could not locate project root (missing requirements.txt)")
        current = parent