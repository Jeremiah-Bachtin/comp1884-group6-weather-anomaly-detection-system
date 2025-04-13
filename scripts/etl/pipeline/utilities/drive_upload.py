# scripts/etl/pipeline/utilities/drive_upload.py
"""
Handles Google Drive authentication and file uploads using PyDrive2.
"""

import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from config.config import CLIENT_SECRETS_PATH

# Location for cached credentials
CREDENTIALS_PATH = os.path.join("scripts", "devtools", "credentials.json")


def authenticate_drive():
    """Authenticate and return an authorised GoogleDrive client."""
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(CLIENT_SECRETS_PATH)

    if os.path.exists(CREDENTIALS_PATH):
        gauth.LoadCredentialsFile(CREDENTIALS_PATH)
    else:
        gauth.LocalWebserverAuth()

    if gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    gauth.SaveCredentialsFile(CREDENTIALS_PATH)
    return GoogleDrive(gauth)


def upload_file_to_drive(local_path, drive_folder_id):
    """
    Upload a file to a specified folder in Google Drive.
    Args:
        local_path (str): Full path to the local file.
        drive_folder_id (str): Google Drive folder ID.
    """
    drive = authenticate_drive()
    file_name = os.path.basename(local_path)

    gfile = drive.CreateFile({
        'title': file_name,
        'parents': [{'id': drive_folder_id}]
    })
    gfile.SetContentFile(local_path)
    gfile.Upload()
    print(f"[INFO] Uploaded to Drive: {file_name} → Folder ID {drive_folder_id}")
