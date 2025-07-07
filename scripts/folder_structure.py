import os
import shutil
from pathlib import Path
import sys

# Set base path to the parent directory of the script folder
base_path = Path(__file__).resolve().parent.parent

def create_folders(base_path=None, structure=None):
    if base_path is None:
        base_path = Path(__file__).resolve().parent.parent  # Folder above the script folder

    if structure is None:
        structure = {
            "Data Store": {
                "Data - CAN": {
                    "002 - Security and Global": {},
                    "003 - Personal to Cascade": {},
                    "004 - Jobs to Cascade": {},
                    "005 - Absences to Cascade": {},
                    "006 - CascadeId to ADP": {},
                },
                "Data - USA": {
                    "002 - Security and Global": {},
                    "003 - Personal to Cascade": {},
                    "004 - Jobs to Cascade": {},
                    "005 - Absences to Cascade": {},
                    "006 - CascadeId to ADP": {},
                },
            }
        }

    for folder_name, subfolders in structure.items():
        # Create the folder
        folder_path = base_path/ folder_name
        os.makedirs(folder_path, exist_ok=True)

        # Recursively create subfolders
        if isinstance(subfolders, dict):
            create_folders(folder_path, subfolders)

def delete_folders():
    base_path_delete = base_path / "Data Store"
    folders_to_delete = ["Data - CAN", "Data - USA"]

    for folder_name in folders_to_delete:
        # Get the folder path
        folder_path = base_path_delete / folder_name
        
        # Delete the folder and its contents recursively if it exists
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)

if __name__ == "__main__":
    create_folders()
    delete_folders()
