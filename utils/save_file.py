import os
import pandas as pd
from utils.find_root import find_project_root

def save_csv(df: pd.DataFrame, filename: str, subdir: str) -> str:
    """
    Save DataFrame to a project-relative subdirectory under /data.
    Returns a full output path.
    """
    output_dir = os.path.join(find_project_root(), "data", subdir)
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    df.to_csv(full_path, index=False)
    return full_path
