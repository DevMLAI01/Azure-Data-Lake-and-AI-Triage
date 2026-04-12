"""
function_app.py — Azure Function entry point
----------------------------------------------
HTTP trigger: POST /api/triage
Accepts anomaly payload from Databricks SQL Alert webhook.
"""

import json
import logging
import azure.functions as func
from agent_runner import run_agent

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

logger = logging.getLogger(__name__)


@app.route(route="triage", methods=["POST"])
def triage_agent(req: func.HttpRequest) -> func.HttpResponse:
    """
    Expected request body (from Databricks SQL Alert webhook):
    {
        "cell_id":  "CELL_0087",
        "metric":   "null_rate_duration",
        "value":    0.45,
        "z_score":  5.8,
        "date":     "2026-03-20",
        "severity": "P1"
    }
    """
    logger.info("Triage agent triggered")

    try:
        anomaly = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Request body must be valid JSON"}),
            status_code=400,
            mimetype="application/json",
        )

    # Basic validation
    required = ["cell_id", "metric", "value", "date"]
    missing  = [k for k in required if k not in anomaly]
    if missing:
        return func.HttpResponse(
            json.dumps({"error": f"Missing required fields: {missing}"}),
            status_code=400,
            mimetype="application/json",
        )

    logger.info(f"Processing anomaly: cell={anomaly['cell_id']} "
                f"metric={anomaly['metric']} value={anomaly['value']}")

    result = run_agent(anomaly)

    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Simple health check — confirms function is running."""
    return func.HttpResponse(
        json.dumps({"status": "healthy", "function": "triage_agent"}),
        status_code=200,
        mimetype="application/json",
    )
