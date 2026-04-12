"""
create_sql_alert.py
-------------------
Phase 8: Creates the Databricks SQL saved query and alert for anomaly detection.

Steps performed:
  1. Find or create a Serverless SQL Warehouse (needed by Databricks SQL Editor/Alerts)
  2. Create saved query  "CDR Null Rate Anomaly — P1 Count"   (returns single int)
  3. Create SQL Alert    "Daily CDR Null Rate P1 Alert"        (fires when p1_count > 0)
  4. Print current p1_count by running the query via Statement Execution API

The alert fires whenever any cell has a CDR null-rate Z-score >= 5 (P1) today.
After Phase 9, wire the Azure Function webhook URL into the alert notification channel.

Usage:
    uv run scripts/create_sql_alert.py            # idempotent create/update
    uv run scripts/create_sql_alert.py --preview  # also print full anomaly table
"""

import os
import sys
import time
import argparse
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql

load_dotenv()

HOST  = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]

WAREHOUSE_NAME = "telecom-sql-wh"
QUERY_NAME     = "CDR Null Rate Anomaly — P1 Count"
ALERT_NAME     = "Daily CDR Null Rate P1 Alert"

# ── SQL that powers the alert ─────────────────────────────────────────────────
# Returns ONE row: { p1_count: <int> }
# Alert fires when p1_count > 0.
ALERT_SQL = """
WITH baseline AS (
    SELECT
        cell_tower_id,
        AVG(null_rate_duration)    AS mean_null_rate,
        STDDEV(null_rate_duration) AS stddev_null_rate
    FROM telecon_dev.gold.daily_network_health
    WHERE event_date < CURRENT_DATE
    GROUP BY cell_tower_id
    HAVING COUNT(*) >= 3
),
latest AS (
    SELECT *
    FROM telecon_dev.gold.daily_network_health
    WHERE event_date = (SELECT MAX(event_date) FROM telecon_dev.gold.daily_network_health)
),
scored AS (
    SELECT
        l.cell_tower_id,
        ABS(
            (l.null_rate_duration - b.mean_null_rate)
            / NULLIF(b.stddev_null_rate, 0)
        ) AS z_score_abs
    FROM latest l
    JOIN baseline b ON l.cell_tower_id = b.cell_tower_id
)
SELECT COUNT(*) AS p1_count
FROM scored
WHERE z_score_abs >= 5
""".strip()

# ── Full detail query (for --preview) ─────────────────────────────────────────
FULL_SQL = """
WITH baseline AS (
    SELECT
        cell_tower_id,
        AVG(null_rate_duration)    AS mean_null_rate,
        STDDEV(null_rate_duration) AS stddev_null_rate
    FROM telecon_dev.gold.daily_network_health
    WHERE event_date < CURRENT_DATE
    GROUP BY cell_tower_id
    HAVING COUNT(*) >= 3
),
latest AS (
    SELECT *
    FROM telecon_dev.gold.daily_network_health
    WHERE event_date = (SELECT MAX(event_date) FROM telecon_dev.gold.daily_network_health)
)
SELECT
    l.event_date,
    l.cell_tower_id,
    ROUND(l.null_rate_duration, 4)                                      AS observed_null_rate,
    ROUND(b.mean_null_rate, 4)                                          AS baseline_mean,
    ROUND(b.stddev_null_rate, 4)                                        AS baseline_stddev,
    ROUND(
        (l.null_rate_duration - b.mean_null_rate)
        / NULLIF(b.stddev_null_rate, 0),
    2)                                                                  AS z_score,
    CASE
        WHEN ABS((l.null_rate_duration - b.mean_null_rate)
             / NULLIF(b.stddev_null_rate, 0)) >= 5  THEN 'P1'
        WHEN ABS((l.null_rate_duration - b.mean_null_rate)
             / NULLIF(b.stddev_null_rate, 0)) >= 3  THEN 'P2'
        WHEN ABS((l.null_rate_duration - b.mean_null_rate)
             / NULLIF(b.stddev_null_rate, 0)) >= 2  THEN 'P3'
        ELSE 'NORMAL'
    END                                                                 AS severity,
    l.total_calls,
    ROUND(l.drop_rate_pct, 4)                                           AS drop_rate_pct
FROM latest l
JOIN baseline b ON l.cell_tower_id = b.cell_tower_id
WHERE
    ABS(
        (l.null_rate_duration - b.mean_null_rate)
        / NULLIF(b.stddev_null_rate, 0)
    ) >= 2
ORDER BY z_score DESC
""".strip()


def get_client() -> WorkspaceClient:
    return WorkspaceClient(host=HOST, token=TOKEN)


# ── SQL Warehouse ─────────────────────────────────────────────────────────────

def find_or_create_warehouse(w: WorkspaceClient) -> str:
    """Returns warehouse_id."""
    for wh in w.warehouses.list():
        if wh.name == WAREHOUSE_NAME:
            print(f"  Found existing warehouse: {wh.name} ({wh.id}), state={wh.state}")
            return wh.id

    print(f"  Creating SQL Warehouse: {WAREHOUSE_NAME} (Serverless, 2X-Small)...")
    created = w.warehouses.create(
        name=WAREHOUSE_NAME,
        cluster_size="2X-Small",
        warehouse_type=dbsql.EndpointInfoWarehouseType.PRO,
        enable_serverless_compute=True,
        auto_stop_mins=30,
        max_num_clusters=1,
    )
    wh_id = created.id   # NOTE: CreateWarehouseResponse has .id, not .warehouse_id
    print(f"  Warehouse created: {wh_id}")
    _wait_for_warehouse(w, wh_id)
    return wh_id


def _wait_for_warehouse(w: WorkspaceClient, wh_id: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    last_state = None
    while time.time() < deadline:
        info = w.warehouses.get(id=wh_id)
        state = str(info.state)
        if state != last_state:
            print(f"  Warehouse status: {state}")
            last_state = state
        if "RUNNING" in state:
            return
        if "STOPPED" in state or "DELETED" in state:
            raise RuntimeError(f"Warehouse entered unexpected state: {state}")
        time.sleep(10)
    raise TimeoutError("Warehouse did not start within timeout")


# ── Saved Query ───────────────────────────────────────────────────────────────

def find_query_id(w: WorkspaceClient) -> str | None:
    for q in w.queries.list():
        if q.display_name == QUERY_NAME:
            return q.id
    return None


def create_or_update_query(w: WorkspaceClient, warehouse_id: str) -> str:
    existing_id = find_query_id(w)

    if existing_id:
        print(f"  Updating existing query (id={existing_id})")
        w.queries.update(
            id=existing_id,
            update_mask="query_text,warehouse_id,description",
            query=dbsql.UpdateQueryRequestQuery(
                query_text=ALERT_SQL,
                warehouse_id=warehouse_id,
                description=(
                    "Returns p1_count: number of cells with CDR null-rate Z-score >= 5 today. "
                    "Alert fires when p1_count > 0."
                ),
            ),
        )
        return existing_id

    print(f"  Creating saved query: {QUERY_NAME!r}")
    created = w.queries.create(
        query=dbsql.CreateQueryRequestQuery(
            display_name=QUERY_NAME,
            query_text=ALERT_SQL,
            warehouse_id=warehouse_id,
            catalog="telecon_dev",
            schema="gold",
            description=(
                "Returns p1_count: number of cells with CDR null-rate Z-score >= 5 today. "
                "Alert fires when p1_count > 0."
            ),
        )
    )
    print(f"  Query created: id={created.id}")
    return created.id


# ── Alert ─────────────────────────────────────────────────────────────────────

def find_alert_id(w: WorkspaceClient) -> str | None:
    for a in w.alerts.list():
        if a.display_name == ALERT_NAME:
            return a.id
    return None


def create_alert(w: WorkspaceClient, query_id: str) -> str:
    existing_id = find_alert_id(w)
    if existing_id:
        print(f"  Alert already exists (id={existing_id}) — skipping")
        return existing_id

    print(f"  Creating alert: {ALERT_NAME!r}")
    created = w.alerts.create(
        alert=dbsql.CreateAlertRequestAlert(
            display_name=ALERT_NAME,
            query_id=query_id,
            condition=dbsql.AlertCondition(
                op=dbsql.AlertOperator.GREATER_THAN,
                operand=dbsql.AlertConditionOperand(
                    column=dbsql.AlertOperandColumn(name="p1_count"),
                ),
                threshold=dbsql.AlertConditionThreshold(
                    value=dbsql.AlertOperandValue(double_value=0),
                ),
            ),
            seconds_to_retrigger=86400,   # re-arm after 24 h
            custom_subject="[TELECOM P1] CDR Null Rate Anomaly Detected",
            custom_body=(
                "One or more cell towers have a CDR null-rate Z-score >= 5 today. "
                "The AI triage agent will investigate automatically via webhook."
            ),
        )
    )
    print(f"  Alert created: id={created.id}")
    return created.id


# ── Preview: run full detail query via Statement Execution API ────────────────

def _execute_and_wait(w: WorkspaceClient, warehouse_id: str,
                      sql: str, timeout: int = 120):
    """Submit SQL and poll until terminal state. Returns the final StatementResponse."""
    stmt = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",   # max allowed inline wait (API limit: 5-50s)
    )
    state = str(stmt.status.state)
    if any(s in state for s in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED")):
        return stmt
    # Still running — poll
    deadline = time.time() + timeout
    while time.time() < deadline:
        stmt = w.statement_execution.get_statement(statement_id=stmt.statement_id)
        state = str(stmt.status.state)
        if any(s in state for s in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED")):
            return stmt
        time.sleep(5)
    return stmt


def run_preview(w: WorkspaceClient, warehouse_id: str) -> None:
    print("\n  Running full anomaly detail query...")
    stmt = _execute_and_wait(w, warehouse_id, FULL_SQL)

    state = str(stmt.status.state)
    if "SUCCEEDED" not in state:
        print(f"  Query state: {state}")
        if stmt.status.error:
            print(f"  Error: {stmt.status.error.message}")
        return

    result = stmt.result
    schema = stmt.manifest.schema.columns if stmt.manifest and stmt.manifest.schema else []
    col_names = [c.name for c in schema]

    if not result or not result.data_array:
        print("  No anomalous cells found today (all Z-scores < 2).")
    else:
        col_w = [max(len(n), 14) for n in col_names]
        header = "  " + "  ".join(n.ljust(cw) for n, cw in zip(col_names, col_w))
        sep    = "  " + "  ".join("-" * cw for cw in col_w)
        print(header)
        print(sep)
        for row in result.data_array:
            print("  " + "  ".join(str(v or "").ljust(cw) for v, cw in zip(row, col_w)))
        print(f"\n  {len(result.data_array)} anomalous cell(s) found.")

    # Run the alert trigger query to show what the alert evaluates to
    print("\n  Running alert trigger query (p1_count)...")
    p1_stmt = _execute_and_wait(w, warehouse_id, ALERT_SQL)
    if "SUCCEEDED" in str(p1_stmt.status.state) and p1_stmt.result and p1_stmt.result.data_array:
        p1_count = p1_stmt.result.data_array[0][0]
        print(f"  p1_count = {p1_count}")
        if int(p1_count or 0) > 0:
            print(f"  -> Alert would FIRE: {p1_count} P1 cell(s) detected today.")
        else:
            print("  -> Alert would NOT fire (no P1 cells today).")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Create Databricks SQL anomaly alert (Phase 8)")
    parser.add_argument("--preview", action="store_true",
                        help="Run the full anomaly detail query and print results")
    args = parser.parse_args()

    w = get_client()
    workspace_host = HOST.rstrip("/")

    print("\n=== Phase 8: SQL Anomaly Alert Setup ===\n")

    # Step 1: SQL Warehouse
    print("Step 1: SQL Warehouse")
    warehouse_id = find_or_create_warehouse(w)
    print()

    # Step 2: Saved query
    print("Step 2: Saved Query")
    query_id = create_or_update_query(w, warehouse_id)
    print(f"  Open in SQL Editor: {workspace_host}/sql/editor/{query_id}")
    print()

    # Step 3: Alert
    print("Step 3: Alert")
    alert_id = create_alert(w, query_id)
    print(f"  Open in Alerts:     {workspace_host}/sql/alerts/{alert_id}")
    print()

    # Summary
    print("=== Setup complete ===\n")
    print(f"  SQL Warehouse : {WAREHOUSE_NAME} ({warehouse_id})")
    print(f"  Saved Query   : {QUERY_NAME!r}")
    print(f"  Alert         : {ALERT_NAME!r}")
    print()
    print("  Manual steps remaining:")
    print("   1. Open the alert URL above -> set schedule to 08:30 UTC daily")
    print("   2. After Phase 9: add webhook notification -> Azure Function URL")
    print()

    # Optional preview
    if args.preview:
        run_preview(w, warehouse_id)


if __name__ == "__main__":
    main()
