setup_cell = """
import io
import os
import sys
import zipfile
import shutil
import re
from google.colab import auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Version to download
VERSION = "0.1.0" # Pass the version of the API


# Define paths
CONTENT_DIR = '/content'
APIS_DIR = os.path.join(CONTENT_DIR, 'APIs')
DBS_DIR = os.path.join(CONTENT_DIR, 'DBs')
SCRIPTS_DIR = os.path.join(CONTENT_DIR, 'Scripts')
FC_DIR = os.path.join(CONTENT_DIR, 'Schemas')
ZIP_PATH = os.path.join(CONTENT_DIR, f'APIs_V{VERSION}.zip')

# Google Drive Folder ID where versioned APIs zip files are stored
APIS_FOLDER_ID = '1QpkAZxXhVFzIbm8qPGPRP1YqXEvJ4uD4'

# List of items to extract from the zip file
ITEMS_TO_EXTRACT = ['APIs/', 'DBs/', 'Scripts/']

# Clean up existing directories and files
for path in [APIS_DIR, DBS_DIR, SCRIPTS_DIR, FC_DIR, ZIP_PATH]:
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

# Authenticate and create the drive service
auth.authenticate_user()
drive_service = build('drive', 'v3')

# Helper function to download a file from Google Drive
def download_drive_file(service, file_id, output_path, file_name=None, show_progress=True):
    "Downloads a file from Google Drive"
    destination = output_path
    request = service.files().get_media(fileId=file_id)
    with io.FileIO(destination, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if show_progress:
                print(f"Download progress: {int(status.progress() * 100)}%")


# 1. List files in the specified APIs folder
print(f"Searching for APIs zip file with version {VERSION} in folder: {APIS_FOLDER_ID}...")
apis_file_id = None

try:
    query = f"'{APIS_FOLDER_ID}' in parents and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    for file in files:
        file_name = file.get('name', '')
        if file_name.lower() == f'apis_v{VERSION.lower()}.zip':
            apis_file_id = file.get('id')
            print(f"Found matching file: {file_name} (ID: {apis_file_id})")
            break

except Exception as e:
    print(f"An error occurred while listing files in Google Drive: {e}")

if not apis_file_id:
    print(f"Error: Could not find APIs zip file with version {VERSION} in the specified folder.")
    sys.exit("Required APIs zip file not found.")

# 2. Download the found APIs zip file
print(f"Downloading APIs zip file with ID: {apis_file_id}...")
download_drive_file(drive_service, apis_file_id, ZIP_PATH, file_name=f'APIs_V{VERSION}.zip')

# 3. Extract specific items from the zip file to /content
print(f"Extracting specific items from {ZIP_PATH} to {CONTENT_DIR}...")
try:
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_contents = zip_ref.namelist()

        for member in zip_contents:
            extracted = False
            for item_prefix in ITEMS_TO_EXTRACT:
              if member == item_prefix or member.startswith(item_prefix):
                    zip_ref.extract(member, CONTENT_DIR)
                    extracted = True
                    break

except zipfile.BadZipFile:
    print(f"Error: The downloaded file at {ZIP_PATH} is not a valid zip file.")
    sys.exit("Invalid zip file downloaded.")
except Exception as e:
    print(f"An error occurred during extraction: {e}")
    sys.exit("Extraction failed.")


# 4. Clean up
if os.path.exists(ZIP_PATH):
    os.remove(ZIP_PATH)

# 5. Add APIs to path
if os.path.exists(APIS_DIR):
    sys.path.append(APIS_DIR)
else:
    print(f"Error: APIS directory not found at {APIS_DIR} after extraction. Cannot add to path.")

# 6. Quick verification
# Check for the presence of the extracted items
verification_paths = [APIS_DIR, DBS_DIR, SCRIPTS_DIR]
all_present = True
print("\nVerifying extracted items:")
for path in verification_paths:
    if os.path.exists(path):
        print(f"✅ {path} is present.")
    else:
        print(f"❌ {path} is MISSING!")
        all_present = False

if all_present:
    print(f"\n✅ Setup complete! Required items extracted to {CONTENT_DIR}.")
else:
    print("\n❌ Setup failed! Not all required items were extracted.")

# 7. Generate Schemas
from Scripts.FCSpec import generate_package_schema

print("\nGenerating FC Schemas")
os.makedirs(FC_DIR, exist_ok=True)

# Change working directory to the source folder
os.chdir(APIS_DIR)

# Iterate through the packages in the /content/APIs directory
for package_name in os.listdir(APIS_DIR):
    package_path = os.path.join(APIS_DIR, package_name)

    # Check if it's a directory (to avoid processing files)
    if os.path.isdir(package_path):
        # Call the function to generate schema for the current package
        generate_package_schema(package_path, output_folder_path=FC_DIR)
print(f"✅ Successfully generated {len(os.listdir(FC_DIR))} FC Schemas to {FC_DIR}")
os.chdir(CONTENT_DIR)

"""
