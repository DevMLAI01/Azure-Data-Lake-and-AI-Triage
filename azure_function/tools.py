"""
tools.py — Agent tool implementations
--------------------------------------
Each function is called by the OpenAI agent during its tool-calling loop.
All tools read from live Azure resources (Databricks SQL, ADLS Gen2).
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient


# ---------------------------------------------------------------------------
# Databricks SQL Statement Execution API helper
# ---------------------------------------------------------------------------

def _run_databricks_sql(sql: str, max_wait_sec: int = 30) -> list[dict]:
    """
    Execute SQL against Databricks SQL Warehouse and return rows as dicts.
    Uses Statement Execution API v2.
    """
    host         = os.environ["DATABRICKS_HOST"].rstrip("/")
    token        = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

    # Submit statement
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement":   sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": f"{max_wait_sec}s",
            "on_wait_timeout": "CONTINUE",
        },
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()

    # Poll if still running
    statement_id = result.get("statement_id")
    deadline     = time.time() + max_wait_sec

    while result.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            raise TimeoutError(f"SQL statement {statement_id} timed out")
        time.sleep(2)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers, timeout=30
        )
        poll.raise_for_status()
        result = poll.json()

    state = result.get("status", {}).get("state")
    if state != "SUCCEEDED":
        err = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed ({state}): {err.get('message','')}")

    manifest = result.get("manifest", {})
    schema   = manifest.get("schema", {}).get("columns", [])
    cols     = [c["name"] for c in schema]
    rows     = result.get("result", {}).get("data_array", [])

    return [dict(zip(cols, row)) for row in rows]


# ---------------------------------------------------------------------------
# ADLS Gen2 helper
# ---------------------------------------------------------------------------

def _get_adls_fs():
    account = os.environ["ADLS_ACCOUNT_NAME"]
    key     = os.environ["ADLS_KEY"]
    client  = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=key
    )
    return client.get_file_system_client(
        os.environ.get("ADLS_CONTAINER", "telecom")
    )


# ---------------------------------------------------------------------------
# Tool 1: get_anomaly_context
# ---------------------------------------------------------------------------

def get_anomaly_context(cell_id: str, event_date: str) -> str:
    """
    Fetch current-day metrics for the flagged cell from gold.daily_network_health.
    Returns JSON string with all key metrics.
    """
    catalog = os.environ.get("DATABRICKS_CATALOG", "telecon_dev")
    rows = _run_databricks_sql(f"""
        SELECT
            cell_tower_id,
            event_date,
            total_calls,
            congestion_drops,
            drop_rate_pct,
            avg_duration_sec,
            null_rate_duration,
            roaming_calls,
            avg_data_volume_mb
        FROM {catalog}.gold.daily_network_health
        WHERE cell_tower_id = '{cell_id}'
          AND event_date    = '{event_date}'
    """)

    if not rows:
        return json.dumps({"error": f"No data found for {cell_id} on {event_date}"})

    return json.dumps(rows[0], default=str)


# ---------------------------------------------------------------------------
# Tool 2: get_metric_trend
# ---------------------------------------------------------------------------

def get_metric_trend(cell_id: str, days: int = 5) -> str:
    """
    Return KPI trend for the last N days for a cell.
    Covers both CDR-based health and KPI performance metrics.
    """
    catalog = os.environ.get("DATABRICKS_CATALOG", "telecon_dev")

    cdr_rows = _run_databricks_sql(f"""
        SELECT
            event_date,
            total_calls,
            drop_rate_pct,
            null_rate_duration,
            avg_duration_sec
        FROM {catalog}.gold.daily_network_health
        WHERE cell_tower_id = '{cell_id}'
        ORDER BY event_date DESC
        LIMIT {days}
    """)

    kpi_rows = _run_databricks_sql(f"""
        SELECT
            event_date,
            technology,
            avg_call_setup_success_rate,
            avg_handover_success_rate,
            avg_dl_throughput_mbps
        FROM {catalog}.gold.kpi_daily_performance
        WHERE cell_id = '{cell_id}'
        ORDER BY event_date DESC
        LIMIT {days}
    """)

    return json.dumps({
        "cell_id":       cell_id,
        "days_requested": days,
        "cdr_trend":     cdr_rows,
        "kpi_trend":     kpi_rows,
    }, default=str)


# ---------------------------------------------------------------------------
# Tool 3: get_noc_history
# ---------------------------------------------------------------------------

def get_noc_history(keyword: str = "", max_notes: int = 3) -> str:
    """
    Search NOC incident notes in ADLS landing/noc_notes/ for relevant context.
    Simple keyword match — returns up to max_notes matching notes.
    """
    fs = _get_adls_fs()
    matches = []

    try:
        paths = list(fs.get_paths(path="landing/noc_notes", recursive=True))
        txt_files = [p.name for p in paths if not p.is_directory and p.name.endswith(".txt")]

        for path in txt_files:
            if len(matches) >= max_notes:
                break
            try:
                file_client = fs.get_file_client(path)
                content     = file_client.download_file().readall().decode("utf-8", errors="replace")
                if not keyword or keyword.lower() in content.lower():
                    matches.append({
                        "file":    path.split("/")[-1],
                        "excerpt": content[:600],
                    })
            except Exception as e:
                logging.warning(f"Could not read {path}: {e}")
    except Exception as e:
        return json.dumps({"error": str(e)})

    if not matches:
        return json.dumps({"message": f"No NOC notes found matching '{keyword}'"})

    return json.dumps({"noc_notes": matches}, default=str)


# ---------------------------------------------------------------------------
# Tool 4: write_incident_report
# ---------------------------------------------------------------------------

def write_incident_report(
    severity:              str,
    cell_id:               str,
    root_cause_hypothesis: str,
    narrative:             str,
    recommended_actions:   list[str],
    confidence:            float,
    event_date:            str = "",
) -> str:
    """
    Write a structured incident report as Markdown to ADLS landing/incidents/.
    Returns the incident ID and ADLS path.
    """
    import uuid
    incident_id = str(uuid.uuid4())[:8].upper()
    ts          = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    actions_md = "\n".join(f"- [ ] {a}" for a in recommended_actions)

    report = f"""# {severity} Incident — {cell_id} — {incident_id}

| Field | Value |
|---|---|
| **Incident ID** | {incident_id} |
| **Severity** | {severity} |
| **Cell** | {cell_id} |
| **Event Date** | {event_date or 'N/A'} |
| **Detected** | {ts} |
| **Confidence** | {confidence:.0%} |

## Root Cause Hypothesis
{root_cause_hypothesis}

## Narrative
{narrative}

## Recommended Actions
{actions_md}

---
*Generated by Azure AI Triage Agent (GPT-4o)*
"""

    fs        = _get_adls_fs()
    adls_path = f"landing/incidents/INCIDENT_{incident_id}.md"
    file_client = fs.get_file_client(adls_path)
    file_client.upload_data(report.encode("utf-8"), overwrite=True)

    logging.info(f"Incident {incident_id} written to {adls_path}")

    return json.dumps({
        "status":      "written",
        "incident_id": incident_id,
        "adls_path":   adls_path,
        "severity":    severity,
    })


# ---------------------------------------------------------------------------
# Tool registry — used by agent_runner.py
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_anomaly_context",
            "description": (
                "Fetch current-day metrics for an anomalous cell from the Gold "
                "network health table. Call this first for any new anomaly."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "cell_id":    {"type": "string", "description": "e.g. CELL_0087"},
                    "event_date": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["cell_id", "event_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_trend",
            "description": (
                "Get CDR and KPI trend for a cell over the last N days. "
                "Use this to distinguish sudden spikes from gradual drift."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "cell_id": {"type": "string"},
                    "days":    {"type": "integer", "default": 5},
                },
                "required": ["cell_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_noc_history",
            "description": (
                "Search past NOC incident notes in ADLS for similar events. "
                "Pass a keyword like 'congestion', 'GPS', or a cell ID."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword":   {"type": "string"},
                    "max_notes": {"type": "integer", "default": 3},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_incident_report",
            "description": (
                "Write a structured incident report to ADLS once root cause "
                "is determined. Call this as the final step."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "severity":              {"type": "string", "enum": ["P1", "P2", "P3"]},
                    "cell_id":               {"type": "string"},
                    "root_cause_hypothesis": {"type": "string"},
                    "narrative":             {"type": "string"},
                    "recommended_actions":   {"type": "array", "items": {"type": "string"}},
                    "confidence":            {"type": "number", "minimum": 0, "maximum": 1},
                    "event_date":            {"type": "string"},
                },
                "required": [
                    "severity", "cell_id", "root_cause_hypothesis",
                    "narrative", "recommended_actions", "confidence",
                ],
            },
        },
    },
]

TOOL_MAP = {
    "get_anomaly_context":  lambda a: get_anomaly_context(**a),
    "get_metric_trend":     lambda a: get_metric_trend(**a),
    "get_noc_history":      lambda a: get_noc_history(**a),
    "write_incident_report":lambda a: write_incident_report(**a),
}
