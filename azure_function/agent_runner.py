"""
agent_runner.py — OpenAI tool-calling agent loop
--------------------------------------------------
Receives an anomaly payload, runs a multi-turn GPT-4o tool-calling loop,
and writes an incident report to ADLS when done.
"""

import os
import json
import logging
from openai import AzureOpenAI
from tools import TOOL_DEFINITIONS, TOOL_MAP

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a telecom NOC triage agent with access to live network data.

When given an anomaly alert:
1. Call get_anomaly_context to get current metrics for the flagged cell
2. Call get_metric_trend to see if this is a sudden spike or gradual drift
3. Call get_noc_history with a relevant keyword to check for similar past incidents
4. Reason about root cause based on all evidence gathered
5. Call write_incident_report with your findings

Rules:
- Always gather context from at least 2 tools before writing the report
- Be specific: name the most likely root cause, not just "possible network issue"
- Confidence score: 0.9+ only if trend + history strongly support hypothesis
- recommended_actions must be concrete and actionable (3-5 items)
- Severity P1 = immediate action, P2 = investigate today, P3 = monitor
"""


def run_agent(anomaly: dict) -> dict:
    """
    Run the triage agent for a single anomaly event.

    anomaly dict expected keys:
        cell_id   : str  — e.g. "CELL_0087"
        metric    : str  — e.g. "null_rate_duration"
        value     : float
        z_score   : float
        date      : str  — YYYY-MM-DD
        severity  : str  — P1 / P2 / P3

    Returns dict with incident_id and final narrative.
    """
    client = AzureOpenAI(
        azure_endpoint = os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key        = os.environ["AZURE_OPENAI_KEY"],
        api_version    = "2024-12-01-preview",
    )
    deployment = os.environ.get("AZURE_OPENAI_GPT4O_DEPLOYMENT", "gpt-4o")

    user_message = (
        f"Anomaly detected — investigate and write an incident report:\n\n"
        f"  Cell ID  : {anomaly.get('cell_id')}\n"
        f"  Metric   : {anomaly.get('metric')}\n"
        f"  Value    : {anomaly.get('value')}\n"
        f"  Z-score  : {anomaly.get('z_score')}\n"
        f"  Date     : {anomaly.get('date')}\n"
        f"  Severity : {anomaly.get('severity', 'P1')}\n"
    )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": user_message},
    ]

    logger.info(f"Starting agent for anomaly: {anomaly}")
    turn = 0
    max_turns = 10  # safety limit

    while turn < max_turns:
        turn += 1
        logger.info(f"Agent turn {turn}")

        response = client.chat.completions.create(
            model       = deployment,
            messages    = messages,
            tools       = TOOL_DEFINITIONS,
            tool_choice = "auto",
        )

        msg = response.choices[0].message

        # No tool calls — agent has finished reasoning
        if not msg.tool_calls:
            logger.info(f"Agent done after {turn} turns")
            return {
                "status":    "complete",
                "turns":     turn,
                "narrative": msg.content,
            }

        # Append assistant message with tool calls
        messages.append(msg)

        # Execute each tool the agent requested
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            fn_args = json.loads(tc.function.arguments)

            logger.info(f"  Tool call: {fn_name}({list(fn_args.keys())})")

            if fn_name not in TOOL_MAP:
                tool_result = json.dumps({"error": f"Unknown tool: {fn_name}"})
            else:
                try:
                    tool_result = TOOL_MAP[fn_name](fn_args)
                except Exception as e:
                    logger.error(f"Tool {fn_name} failed: {e}")
                    tool_result = json.dumps({"error": str(e)})

            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      tool_result,
            })

    logger.warning(f"Agent hit max_turns={max_turns} without finishing")
    return {"status": "max_turns_reached", "turns": turn}
