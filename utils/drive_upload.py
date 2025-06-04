import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from config.original_config import CLIENT_SECRETS_PATH

CREDENTIALS_PATH = "credentials.json"  # or store elsewhere if needed

def authenticate_drive():
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(CLIENT_SECRETS_PATH)

    # Try to load existing saved credentials
    if os.path.exists(CREDENTIALS_PATH):
        gauth.LoadCredentialsFile(CREDENTIALS_PATH)
    else:
        gauth.LocalWebserverAuth()  # first-time login

    if gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    # Save credentials for reuse
    gauth.SaveCredentialsFile(CREDENTIALS_PATH)

    return GoogleDrive(gauth)

def upload_file_to_drive(local_path, drive_folder_id, folder_name="Unknown Folder"):
    drive = authenticate_drive()
    file_name = os.path.basename(local_path)

    file_drive = drive.CreateFile({'title': file_name, 'parents': [{'id': drive_folder_id}]})
    file_drive.SetContentFile(local_path)
    file_drive.Upload()
    print(f"[INFO] Uploaded to Drive: {file_name} → '{folder_name}' (ID: {drive_folder_id})")

