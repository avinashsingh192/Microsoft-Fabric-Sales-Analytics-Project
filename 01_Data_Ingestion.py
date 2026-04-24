import zipfile
import os

# --- Configuration ---
# LAKEHOUSE_FILES_DIR points to the standard Fabric Lakehouse file path.
# If running locally in VS Code, ensure you have access to this path 
# or update it to a local directory.
LAKEHOUSE_FILES_DIR = "/lakehouse/default/Files/imported_files"
SOURCE_ZIP_NAME = "wwi-sample-dataset.zip"

# Construct the full paths for the source file and the extraction destination
source_file_path = f"{LAKEHOUSE_FILES_DIR}/{SOURCE_ZIP_NAME}"
destination_path = LAKEHOUSE_FILES_DIR

def extract_dataset(source, destination):
    """
    Extracts the WWI sample dataset zip file to the specified destination.
    """
    print(f"Starting extraction process for: {SOURCE_ZIP_NAME}")

    # Safety check: Ensure the source file actually exists before proceeding.
    if os.path.exists(source):
        try:
            # The 'with' statement ensures the file is properly closed after use.
            with zipfile.ZipFile(source, 'r') as zip_archive:
                # Extract all contents of the zip archive to the destination folder.
                zip_archive.extractall(path=destination)
            
            print(f"Success: Data extracted to {destination}")
        except zipfile.BadZipFile:
            print(f"ERROR: The file at {source} is not a valid zip file.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
        print(f"ERROR: Source file not found at {source}. Please check the path and filename.")

if __name__ == "__main__":
    extract_dataset(source_file_path, destination_path)