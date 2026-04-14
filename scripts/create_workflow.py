"""
create_workflow.py
------------------
Creates (or recreates) the 'daily_medallion_pipeline' Databricks Workflow.

The DAG has three stages with proper task dependencies:
  Stage 1 — Bronze  (6 tasks, run in parallel)
  Stage 2 — Silver  (2 tasks, each depends on all 6 bronze tasks)
  Stage 3 — Gold    (3 tasks, each depends on all 2 silver tasks)

Each task spins up its own ephemeral job cluster (spot + on-demand fallback)
that terminates automatically on completion — no always-on cluster needed.
The job is scheduled daily at 06:00 UTC but starts PAUSED; trigger manually.

Usage:
    uv run scripts/create_workflow.py           # create/update workflow
    uv run scripts/create_workflow.py --run     # create + trigger a manual run
"""

import os
import sys
import time
import argparse
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs  # noqa: F401

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]

WORKFLOW_NAME = "daily_medallion_pipeline"
DATABRICKS_DIR = "/telecom-triage"  # where notebooks were uploaded by run_notebook.py

# All notebook paths on the Databricks workspace (without .py extension)
BRONZE_NOTEBOOKS = [
    "01_bronze_cdr",
    "01_bronze_kpi",
    "01_bronze_snmp",
    "01_bronze_customers",
    "01_bronze_noc_notes",
    "01_bronze_transcripts",
]
SILVER_NOTEBOOKS = [
    "02_silver_cdr",
    "02_silver_kpi",
]
GOLD_NOTEBOOKS = [
    "03_gold_network_health",
    "03_gold_kpi_performance",
    "03_gold_customer_experience",
]


def get_client() -> WorkspaceClient:
    return WorkspaceClient(host=HOST, token=TOKEN)


def get_spark_version(w: WorkspaceClient) -> str:
    """Return the latest LTS Spark runtime available in the workspace."""
    versions = w.clusters.spark_versions().versions or []
    lts = [
        v
        for v in versions
        if v.key and "lts" in v.key.lower() and "scala2.12" in v.key.lower()
    ]
    if lts:
        return lts[0].key
    return "15.4.x-scala2.12"  # safe fallback


def make_job_cluster_spec(spark_version: str) -> compute.ClusterSpec:
    """Ephemeral spot cluster — spins up for the task and terminates on completion."""
    return compute.ClusterSpec(
        spark_version=spark_version,
        node_type_id="Standard_DS3_v2",  # 14 GB RAM, 4 vCPUs — right-sized for this data volume
        num_workers=1,
        azure_attributes=compute.AzureAttributes(
            availability=compute.AzureAvailability.SPOT_WITH_FALLBACK_AZURE,
            spot_bid_max_price=-1,  # cap at on-demand price
            first_on_demand=1,  # driver node always on-demand for stability
        ),
    )


def notebook_path(name: str) -> str:
    return f"{DATABRICKS_DIR}/{name}"


def make_task(
    task_key: str,
    notebook_name: str,
    spark_version: str,
    depends_on: list[str] | None = None,
) -> jobs.Task:
    deps = [jobs.TaskDependency(task_key=k) for k in (depends_on or [])]
    return jobs.Task(
        task_key=task_key,
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook_path(notebook_name),
        ),
        new_cluster=make_job_cluster_spec(spark_version),  # ephemeral spot cluster
        depends_on=deps if deps else None,
    )


def delete_existing(w: WorkspaceClient) -> None:
    """Delete the workflow if it already exists (idempotent recreate)."""
    for job in w.jobs.list(name=WORKFLOW_NAME):
        print(f"  Deleting existing workflow (job_id={job.job_id})")
        w.jobs.delete(job_id=job.job_id)


def create_workflow(w: WorkspaceClient, spark_version: str) -> int:
    # --- Stage 1: Bronze (all parallel, no dependencies) ---
    bronze_tasks = [
        make_task(f"bronze_{nb.replace('01_bronze_', '')}", nb, spark_version)
        for nb in BRONZE_NOTEBOOKS
    ]
    bronze_keys = [t.task_key for t in bronze_tasks]

    # --- Stage 2: Silver (each depends on ALL bronze tasks) ---
    silver_tasks = [
        make_task(
            f"silver_{nb.replace('02_silver_', '')}",
            nb,
            spark_version,
            depends_on=bronze_keys,
        )
        for nb in SILVER_NOTEBOOKS
    ]
    silver_keys = [t.task_key for t in silver_tasks]

    # --- Stage 3: Gold (each depends on ALL silver tasks) ---
    gold_tasks = [
        make_task(
            f"gold_{nb.replace('03_gold_', '')}",
            nb,
            spark_version,
            depends_on=silver_keys,
        )
        for nb in GOLD_NOTEBOOKS
    ]

    all_tasks = bronze_tasks + silver_tasks + gold_tasks

    created = w.jobs.create(
        name=WORKFLOW_NAME,
        tasks=all_tasks,
        schedule=jobs.CronSchedule(
            quartz_cron_expression="0 0 6 * * ?",  # 06:00 UTC daily
            timezone_id="UTC",
            pause_status=jobs.PauseStatus.PAUSED,  # start paused; unpause when ready
        ),
        max_concurrent_runs=1,
    )

    job_id = created.job_id
    workspace_host = HOST.rstrip("/")
    url = f"{workspace_host}/#job/{job_id}"
    print(f"\n  Workflow created: {WORKFLOW_NAME}")
    print(f"  Job ID  : {job_id}")
    print(f"  URL     : {url}")
    print(f"\n  Tasks ({len(all_tasks)}):")
    print(f"    Bronze  ({len(bronze_tasks)} parallel) : {', '.join(bronze_keys)}")
    print(f"    Silver  ({len(silver_tasks)}, after bronze): {', '.join(silver_keys)}")
    print(
        f"    Gold    ({len(gold_tasks)}, after silver): {', '.join([t.task_key for t in gold_tasks])}"
    )
    print(
        "\n  Schedule: 06:00 UTC daily (currently PAUSED — unpause in Databricks UI when ready)"
    )
    return job_id


def trigger_run(w: WorkspaceClient, job_id: int) -> None:
    print(f"\n  Triggering manual run of job {job_id}...")
    run_response = w.jobs.run_now(job_id=job_id)
    run_id = run_response.run_id
    workspace_host = HOST.rstrip("/")
    print(f"  Run ID  : {run_id}")
    print(f"  Run URL : {workspace_host}/#job/{job_id}/run/{run_id}")

    print("\n  Polling run status (this will take several minutes)...")
    last_state = None
    deadline = time.time() + 7200  # 2-hour timeout

    while time.time() < deadline:
        run_info = w.jobs.get_run(run_id=run_id)
        state_str = str(run_info.state.life_cycle_state)

        if state_str != last_state:
            print(f"  Status: {state_str}")
            last_state = state_str

        if any(s in state_str for s in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")):
            result = str(run_info.state.result_state)
            if "SUCCESS" in result:
                print("\n  Run SUCCEEDED.")
            else:
                msg = run_info.state.state_message or ""
                print(f"\n  Run FAILED — {msg}")
                # Print failed task details
                for task in run_info.tasks or []:
                    task_state = str(task.state.result_state) if task.state else ""
                    if "FAILED" in task_state or "TIMEDOUT" in task_state:
                        print(f"    Failed task: {task.task_key}")
                        try:
                            out = w.jobs.get_run_output(run_id=task.run_id)
                            if out.error:
                                print(f"    Error: {out.error}")
                            if out.notebook_output:
                                print(f"    Output:\n{out.notebook_output.result}")
                        except Exception:
                            pass
                sys.exit(1)
            return

        time.sleep(15)

    print("  TIMEOUT: run did not complete within 2 hours")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Create the daily_medallion_pipeline Databricks Workflow"
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Trigger a manual run immediately after creating the workflow",
    )
    args = parser.parse_args()

    w = get_client()
    spark_version = get_spark_version(w)
    print(f"  Spark runtime: {spark_version}")

    print("\nRemoving any existing workflow with the same name...")
    delete_existing(w)

    print("\nCreating workflow (job clusters — spot + on-demand fallback)...")
    job_id = create_workflow(w, spark_version)

    if args.run:
        trigger_run(w, job_id)


if __name__ == "__main__":
    main()
