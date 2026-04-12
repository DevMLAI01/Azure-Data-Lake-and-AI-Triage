"""
run_notebook.py
---------------
Uploads a local notebook to Databricks and runs it on the cluster.
Polls until completion and prints cell output.

Usage:
    uv run scripts/run_notebook.py --notebook notebooks/01_bronze_cdr.py
    uv run scripts/run_notebook.py --all-bronze
    uv run scripts/run_notebook.py --all-silver
    uv run scripts/run_notebook.py --all-gold
"""

import os
import sys
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace

load_dotenv()

HOST       = os.environ["DATABRICKS_HOST"]
TOKEN      = os.environ["DATABRICKS_TOKEN"]
CATALOG    = os.environ.get("DATABRICKS_CATALOG", "telecon_dev")

NOTEBOOKS_DIR  = Path(__file__).parent.parent / "notebooks"
DATABRICKS_DIR = "/telecom-triage"   # folder in Databricks workspace

PIPELINE_ORDER = {
    "bronze": [
        "01_bronze_cdr.py",
        "01_bronze_kpi.py",
        "01_bronze_snmp.py",
        "01_bronze_customers.py",
        "01_bronze_noc_notes.py",
        "01_bronze_transcripts.py",
    ],
    "silver": [
        "02_silver_cdr.py",
        "02_silver_kpi.py",
    ],
    "gold": [
        "03_gold_network_health.py",
        "03_gold_kpi_performance.py",
        "03_gold_customer_experience.py",
    ],
}


def get_client() -> WorkspaceClient:
    return WorkspaceClient(host=HOST, token=TOKEN)


def get_cluster_id(w: WorkspaceClient) -> str:
    """Find the first running or available cluster."""
    clusters = list(w.clusters.list())
    if not clusters:
        raise RuntimeError("No clusters found. Create a cluster in Databricks first.")

    # Prefer running clusters
    for c in clusters:
        state = str(c.state).upper()
        if "RUNNING" in state:
            print(f"  Using running cluster: {c.cluster_name} ({c.cluster_id})")
            return c.cluster_id

    # Fall back to first available (will auto-start)
    c = clusters[0]
    print(f"  Using cluster: {c.cluster_name} ({c.cluster_id}) — will start if needed")
    return c.cluster_id


def upload_notebook(w: WorkspaceClient, local_path: Path) -> str:
    """Upload .py notebook to Databricks workspace. Returns remote path."""
    import base64
    remote_path = f"{DATABRICKS_DIR}/{local_path.name.replace('.py', '')}"

    with open(local_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode("utf-8")

    w.workspace.import_(
        path      = remote_path,
        format    = workspace.ImportFormat.SOURCE,
        language  = workspace.Language.PYTHON,
        content   = content_b64,
        overwrite = True,
    )
    print(f"  Uploaded: {local_path.name} -> {remote_path}")
    return remote_path


def run_notebook(w: WorkspaceClient, remote_path: str, cluster_id: str,
                 timeout_sec: int = 1800) -> bool:
    """Submit notebook as a one-time run and poll until done. Returns True on success."""
    run = w.jobs.submit(
        run_name = f"claude-run-{Path(remote_path).name}-{int(time.time())}",
        tasks    = [
            jobs.SubmitTask(
                task_key      = "run",
                notebook_task = jobs.NotebookTask(notebook_path=remote_path),
                existing_cluster_id = cluster_id,
            )
        ],
    ).result(timeout=timeout_sec)

    return True


def run_with_polling(w: WorkspaceClient, remote_path: str,
                     cluster_id: str, timeout_sec: int = 1800) -> bool:
    """Submit and poll manually to show live status."""
    print(f"  Submitting run...")
    response = w.jobs.submit(
        run_name = f"claude-run-{Path(remote_path).name}-{int(time.time())}",
        tasks    = [
            jobs.SubmitTask(
                task_key            = "run",
                notebook_task       = jobs.NotebookTask(notebook_path=remote_path),
                existing_cluster_id = cluster_id,
            )
        ],
    )
    run_id = response.run_id
    print(f"  Run ID: {run_id}")

    deadline = time.time() + timeout_sec
    last_state = None

    while time.time() < deadline:
        run_info  = w.jobs.get_run(run_id=run_id)
        state     = run_info.state.life_cycle_state
        state_str = str(state)

        if state_str != last_state:
            print(f"  Status: {state_str}")
            last_state = state_str

        if "TERMINATED" in state_str or "SKIPPED" in state_str or "INTERNAL_ERROR" in state_str:
            result = str(run_info.state.result_state)
            if "SUCCESS" in result:
                print(f"  Result: SUCCESS")
                return True
            else:
                msg = run_info.state.state_message or ""
                print(f"  Result: FAILED — {msg}")
                # Print task output for debugging
                for task in (run_info.tasks or []):
                    task_run = w.jobs.get_run_output(run_id=task.run_id)
                    if task_run.error:
                        print(f"  Error: {task_run.error}")
                    if task_run.notebook_output:
                        print(f"  Output:\n{task_run.notebook_output.result}")
                return False

        time.sleep(10)

    print(f"  TIMEOUT after {timeout_sec}s")
    return False


def run_single(notebook_file: str):
    local_path = NOTEBOOKS_DIR / notebook_file
    if not local_path.exists():
        print(f"ERROR: {local_path} not found")
        sys.exit(1)

    w          = get_client()
    cluster_id = get_cluster_id(w)

    # Ensure workspace folder exists
    try:
        w.workspace.mkdirs(path=DATABRICKS_DIR)
    except Exception:
        pass

    print(f"\nRunning: {notebook_file}")
    remote = upload_notebook(w, local_path)
    ok     = run_with_polling(w, remote, cluster_id)

    if ok:
        print(f"  DONE: {notebook_file}\n")
    else:
        print(f"  FAILED: {notebook_file}\n")
        sys.exit(1)


def run_stage(stage: str):
    notebooks = PIPELINE_ORDER.get(stage)
    if not notebooks:
        print(f"Unknown stage: {stage}. Choose from: bronze, silver, gold")
        sys.exit(1)

    w          = get_client()
    cluster_id = get_cluster_id(w)

    try:
        w.workspace.mkdirs(path=DATABRICKS_DIR)
    except Exception:
        pass

    print(f"\n=== Running {stage.upper()} stage ({len(notebooks)} notebooks) ===\n")
    for nb in notebooks:
        local_path = NOTEBOOKS_DIR / nb
        print(f"Running: {nb}")
        remote = upload_notebook(w, local_path)
        ok     = run_with_polling(w, remote, cluster_id)
        if not ok:
            print(f"\nPipeline stopped at: {nb}")
            sys.exit(1)

    print(f"\n=== {stage.upper()} stage complete ===\n")


def main():
    parser = argparse.ArgumentParser(description="Run Databricks notebooks from Claude Code")
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--notebook",    help="Single notebook file name, e.g. 01_bronze_cdr.py")
    group.add_argument("--all-bronze",  action="store_true")
    group.add_argument("--all-silver",  action="store_true")
    group.add_argument("--all-gold",    action="store_true")
    group.add_argument("--full-pipeline", action="store_true",
                       help="Run bronze -> silver -> gold in sequence")
    args = parser.parse_args()

    if args.notebook:
        run_single(args.notebook)
    elif args.all_bronze:
        run_stage("bronze")
    elif args.all_silver:
        run_stage("silver")
    elif args.all_gold:
        run_stage("gold")
    elif args.full_pipeline:
        run_stage("bronze")
        run_stage("silver")
        run_stage("gold")
        print("Full pipeline complete.")


if __name__ == "__main__":
    main()
