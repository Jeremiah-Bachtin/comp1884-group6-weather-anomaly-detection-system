"""
Google Drive uploader using PyDrive2
Requires manual OAuth the first time.
"""

import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Authenticate and create drive client
def authenticate_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # opens browser for first-time login
    drive = GoogleDrive(gauth)
    return drive

# Upload a local file to a specific folder ID
def upload_file_to_drive(local_path, drive_folder_id):
    drive = authenticate_drive()
    file_name = os.path.basename(local_path)

    file_drive = drive.CreateFile({'title': file_name, 'parents': [{'id': drive_folder_id}]})
    file_drive.SetContentFile(local_path)
    file_drive.Upload()
    print(f"[INFO] Uploaded to Drive: {file_name} -> Folder ID {drive_folder_id}")
