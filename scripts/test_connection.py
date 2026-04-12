"""
test_connection.py
------------------
Verifies ADLS Gen2 connectivity and lists landing/ folders.
Run after upload_to_adls.py to confirm everything arrived.

Usage:
    py -3 scripts/test_connection.py
"""

import os
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

ACCOUNT_NAME = os.environ["ADLS_ACCOUNT_NAME"]
ACCOUNT_KEY  = os.environ["ADLS_KEY"]
CONTAINER    = os.environ.get("ADLS_CONTAINER", "telecom")


def main():
    print(f"Connecting to https://{ACCOUNT_NAME}.dfs.core.windows.net ...\n")

    client    = DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ACCOUNT_KEY
    )
    fs_client = client.get_file_system_client(CONTAINER)

    # List top-level landing folders and count files in each
    print(f"Container: {CONTAINER}/landing/\n")
    print(f"{'Folder':<35} {'Files':>6}")
    print("-" * 42)

    landing_folders = [
        "landing/cdr", "landing/customers", "landing/inventory",
        "landing/kpi", "landing/snmp",      "landing/mediation",
        "landing/app_events", "landing/noc_notes",
        "landing/transcripts","landing/syslogs",  "landing/pdfs",
    ]

    total = 0
    for folder in landing_folders:
        try:
            paths = list(fs_client.get_paths(path=folder, recursive=True))
            files = [p for p in paths if not p.is_directory]
            print(f"  {folder:<33} {len(files):>6}")
            total += len(files)
        except Exception:
            print(f"  {folder:<33} {'NOT FOUND':>6}")

    print("-" * 42)
    print(f"  {'TOTAL':<33} {total:>6}")
    print("\nConnection successful" if total > 0 else "\nNo files found - check upload")


if __name__ == "__main__":
    main()
