"""
upload_to_adls.py
-----------------
Uploads all raw telecom data from S-Drive into ADLS Gen2 landing zone.
Run once after Phase 1 (Azure resources created).

Usage:
    uv pip install -r requirements.txt
    cp .env.example .env          # fill in ADLS_ACCOUNT_NAME and ADLS_KEY
    uv run scripts/upload_to_adls.py
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient
from tqdm import tqdm

load_dotenv()

ACCOUNT_NAME = os.environ["ADLS_ACCOUNT_NAME"]
ACCOUNT_KEY  = os.environ["ADLS_KEY"]
CONTAINER    = os.environ.get("ADLS_CONTAINER", "telecom")
RAW_ROOT     = Path(os.environ.get(
    "RAW_DATA_PATH",
    r"S:\My Projects\Data Lake and AI Triage System\data\raw"
))

# Map: local subfolder (relative to RAW_ROOT) → ADLS landing prefix
UPLOAD_MAP = {
    "structured/cdr":               "landing/cdr",
    "structured/customers":         "landing/customers",
    "structured/inventory":         "landing/inventory",
    "semi_structured/kpi":          "landing/kpi",
    "semi_structured/snmp":         "landing/snmp",
    "semi_structured/mediation":    "landing/mediation",
    "semi_structured/app_events":   "landing/app_events",
    "unstructured/noc_notes":       "landing/noc_notes",
    "unstructured/transcripts":     "landing/transcripts",
    "unstructured/syslogs":         "landing/syslogs",
    "unstructured/maintenance_pdfs":"landing/pdfs",
}


def get_client() -> DataLakeServiceClient:
    return DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ACCOUNT_KEY
    )


def collect_files(upload_map: dict) -> list[tuple[Path, str]]:
    """Return list of (local_path, adls_path) pairs."""
    pairs = []
    for local_rel, adls_prefix in upload_map.items():
        local_dir = RAW_ROOT / Path(local_rel.replace("/", os.sep))
        if not local_dir.exists():
            print(f"  [SKIP] {local_dir} not found")
            continue
        for f in local_dir.rglob("*"):
            if f.is_file():
                relative  = f.relative_to(local_dir)
                adls_path = f"{adls_prefix}/{relative.as_posix()}"
                pairs.append((f, adls_path))
    return pairs


def upload_file(fs_client, local_path: Path, adls_path: str) -> None:
    file_client = fs_client.get_file_client(adls_path)
    with open(local_path, "rb") as fh:
        data = fh.read()
    file_client.upload_data(data, overwrite=True, length=len(data))


def main():
    print(f"Source : {RAW_ROOT}")
    print(f"Target : https://{ACCOUNT_NAME}.dfs.core.windows.net/{CONTAINER}/landing/\n")

    client    = get_client()
    fs_client = client.get_file_system_client(CONTAINER)

    pairs  = collect_files(UPLOAD_MAP)
    errors = []

    print(f"Found {len(pairs)} files to upload\n")

    for local_path, adls_path in tqdm(pairs, unit="file"):
        try:
            upload_file(fs_client, local_path, adls_path)
        except Exception as e:
            errors.append((adls_path, str(e)))

    print(f"\nUploaded : {len(pairs) - len(errors)}")
    if errors:
        print(f"Failed   : {len(errors)}")
        for path, err in errors:
            print(f"   {path}: {err}")
    else:
        print("All files uploaded successfully.")
        print("\nNext: open Databricks workspace and run notebook 00_mount_adls.py")


if __name__ == "__main__":
    main()
