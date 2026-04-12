"""
test_agent_local.py
--------------------
Test the triage agent locally before deploying to Azure Functions.
Requires .env with all keys filled in.

Usage:
    cd azure_function
    py -3 ../scripts/test_agent_local.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "azure_function"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from agent_runner import run_agent

# Simulate what Databricks SQL Alert sends
test_anomaly = {
    "cell_id":  "CELL_0087",
    "metric":   "null_rate_duration",
    "value":    0.45,
    "z_score":  5.8,
    "date":     "2026-03-20",
    "severity": "P1",
}

print("=" * 60)
print("Running triage agent locally...")
print(f"Anomaly: {test_anomaly}")
print("=" * 60)

result = run_agent(test_anomaly)

print("\n=== AGENT RESULT ===")
print(f"Status : {result.get('status')}")
print(f"Turns  : {result.get('turns')}")
print(f"\nNarrative:\n{result.get('narrative', 'N/A')}")
print("\nCheck ADLS landing/incidents/ for the written report.")
