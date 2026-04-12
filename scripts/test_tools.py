"""
test_tools.py
-------------
Verifies all 4 agent tools work end-to-end against live Azure resources.
Does NOT require Azure OpenAI quota — tests the Databricks SQL and ADLS
plumbing independently from the GPT-4o agent loop.

Usage:
    uv run scripts/test_tools.py

All 4 tools must pass before deploying the Function App.
"""

import sys
import os
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "azure_function"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from tools import get_anomaly_context, get_metric_trend, get_noc_history, write_incident_report

PASS = "[PASS]"
FAIL = "[FAIL]"


def run_test(name, fn):
    print(f"\n{'='*60}")
    print(f"Tool: {name}")
    print('='*60)
    try:
        result = fn()
        data = json.loads(result)
        if "error" in data:
            print(f"{FAIL}  Error returned: {data['error']}")
            return False
        print(json.dumps(data, indent=2, default=str)[:1200])
        print(f"\n{PASS}  {name} succeeded")
        return True
    except Exception as e:
        print(f"{FAIL}  Exception: {e}")
        return False


def main():
    print("\nTelecom AI Triage Agent — Tool Verification")
    print("=" * 60)

    # Find a real cell ID and date from the gold table
    print("\nFetching a real cell_id and event_date from gold.daily_network_health...")
    try:
        from tools import _run_databricks_sql
        rows = _run_databricks_sql("""
            SELECT cell_tower_id, event_date, null_rate_duration
            FROM telecon_dev.gold.daily_network_health
            ORDER BY null_rate_duration DESC
            LIMIT 1
        """)
        if not rows:
            print(f"{FAIL}  No rows in gold.daily_network_health — run the gold pipeline first")
            sys.exit(1)
        cell_id    = rows[0]["cell_tower_id"]
        event_date = str(rows[0]["event_date"])
        null_rate  = rows[0]["null_rate_duration"]
        print(f"  Using cell_id={cell_id}  event_date={event_date}  null_rate={null_rate}")
    except Exception as e:
        print(f"{FAIL}  Could not fetch cell data: {e}")
        sys.exit(1)

    results = {}

    # Tool 1: get_anomaly_context
    results["get_anomaly_context"] = run_test(
        "get_anomaly_context",
        lambda: get_anomaly_context(cell_id=cell_id, event_date=event_date),
    )

    # Tool 2: get_metric_trend
    results["get_metric_trend"] = run_test(
        "get_metric_trend",
        lambda: get_metric_trend(cell_id=cell_id, days=5),
    )

    # Tool 3: get_noc_history
    results["get_noc_history"] = run_test(
        "get_noc_history",
        lambda: get_noc_history(keyword="congestion", max_notes=2),
    )

    # Tool 4: write_incident_report (uses a test prefix to avoid polluting real incidents)
    results["write_incident_report"] = run_test(
        "write_incident_report",
        lambda: write_incident_report(
            severity="P2",
            cell_id=cell_id,
            root_cause_hypothesis=(
                "TEST ONLY: Elevated null_rate_duration likely caused by "
                "backhaul congestion during peak hours."
            ),
            narrative=(
                f"Cell {cell_id} shows elevated null_rate_duration on {event_date}. "
                "Trend analysis shows gradual increase over 3 days. NOC notes reference "
                "a nearby fiber maintenance window. Confidence is moderate."
            ),
            recommended_actions=[
                "Check backhaul utilisation on transmission segment",
                "Review maintenance schedule for planned outages",
                "Monitor for next 24 hours — escalate to P1 if rate exceeds 0.5",
            ],
            confidence=0.75,
            event_date=event_date,
        ),
    )

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print('='*60)
    passed = sum(1 for v in results.values() if v)
    total  = len(results)
    for tool, ok in results.items():
        status = PASS if ok else FAIL
        print(f"  {status}  {tool}")
    print(f"\n{passed}/{total} tools passed")

    if passed == total:
        print("\nAll tools verified. The agent is ready to deploy.")
        print("Next step: request Azure OpenAI GPT-4o quota at")
        print("  portal.azure.com -> Azure OpenAI -> Quota -> Request increase")
    else:
        print("\nFix failing tools before deploying.")
        sys.exit(1)


if __name__ == "__main__":
    main()
