import os
from scripts.etl.pipeline.utilities.drive_upload import upload_file_to_drive
from config.config import DRIVE_FOLDER_HISTORICAL  # Swap for any other test folder

def main():
    # Create a test file if it doesn't exist
    test_path = "tests/sample_upload_test.txt"
    os.makedirs(os.path.dirname(test_path), exist_ok=True)
    with open(test_path, "w") as f:
        f.write("This is a test upload to Google Drive.\n")

    print("[INFO] Attempting to upload test file...")
    upload_file_to_drive(test_path,  DRIVE_FOLDER_HISTORICAL, "historical")

if __name__ == "__main__":
    main()