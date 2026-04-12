"""
create_dashboard.py
--------------------
Phase 10: Creates a Databricks SQL Dashboard with 3 pages of telecom analytics
visualisations connected to the Gold tables.

Queries created:
  1. Network Health — null_rate_duration + drop_rate_pct trend by cell/date
  2. Top Cells by Anomaly — cells with highest average null_rate_duration (last 30 days)
  3. KPI Performance — throughput + handover rate trend
  4. P1 Anomaly Feed — cells with Z-score P1 alerts today

Dashboard: "Telecom NOC Analytics"

Usage:
    uv run scripts/create_dashboard.py          # create / update
    uv run scripts/create_dashboard.py --url    # print dashboard URL at the end
"""

import os
import sys
import json
import argparse
import subprocess
import requests
from dotenv import load_dotenv

load_dotenv()

HOST       = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN      = os.environ["DATABRICKS_TOKEN"]
CATALOG    = os.environ.get("DATABRICKS_CATALOG", "telecon_dev")
WH_ID      = os.environ["DATABRICKS_WAREHOUSE_ID"]

# data_source_id is different from warehouse_id — look up via /api/2.0/preview/sql/data_sources
# This is resolved at runtime in get_data_source_id()
_DATA_SOURCE_ID: str = ""

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
}

# Lakeview dashboards require an Azure AD token (PAT doesn't have "dashboards" scope)
_AD_TOKEN: str = ""


def get_ad_token() -> str:
    global _AD_TOKEN
    if _AD_TOKEN:
        return _AD_TOKEN
    # On Windows az is a .cmd file, so shell=True is required
    result = subprocess.run(
        "az account get-access-token "
        "--resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d "
        "--query accessToken -o tsv",
        capture_output=True, text=True, check=True, shell=True,
    )
    _AD_TOKEN = result.stdout.strip()
    return _AD_TOKEN


def lakeview_headers() -> dict:
    return {"Authorization": f"Bearer {get_ad_token()}", "Content-Type": "application/json"}

DASHBOARD_NAME = "Telecom NOC Analytics"

# ── SQL Queries ───────────────────────────────────────────────────────────────

QUERIES = [
    {
        "name": "NOC — Network Health Trend",
        "description": "null_rate_duration and drop_rate_pct per cell per day (last 30 days)",
        "query": f"""
SELECT
    event_date,
    cell_tower_id,
    ROUND(null_rate_duration * 100, 2)  AS null_rate_pct,
    ROUND(drop_rate_pct     * 100, 2)  AS drop_rate_pct,
    total_calls
FROM {CATALOG}.gold.daily_network_health
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY event_date DESC, null_rate_pct DESC
""".strip(),
    },
    {
        "name": "NOC — Top 10 Worst Cells (Null Rate)",
        "description": "Cells with highest average null_rate_duration over the last 30 days",
        "query": f"""
SELECT
    cell_tower_id,
    ROUND(AVG(null_rate_duration) * 100, 2) AS avg_null_rate_pct,
    ROUND(AVG(drop_rate_pct)      * 100, 2) AS avg_drop_rate_pct,
    SUM(total_calls)                        AS total_calls_30d
FROM {CATALOG}.gold.daily_network_health
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY cell_tower_id
ORDER BY avg_null_rate_pct DESC
LIMIT 10
""".strip(),
    },
    {
        "name": "NOC — KPI Performance Trend",
        "description": "Avg DL/UL throughput and handover/call-setup success rates per day",
        "query": f"""
SELECT
    event_date,
    technology,
    ROUND(AVG(avg_dl_throughput_mbps),  2) AS dl_mbps,
    ROUND(AVG(avg_ul_throughput_mbps),  2) AS ul_mbps,
    ROUND(AVG(avg_handover_success_rate)  * 100, 2) AS handover_pct,
    ROUND(AVG(avg_call_setup_success_rate)* 100, 2) AS call_setup_pct
FROM {CATALOG}.gold.kpi_daily_performance
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY event_date, technology
ORDER BY event_date DESC, technology
""".strip(),
    },
    {
        "name": "NOC — P1 Anomaly Feed",
        "description": "Cells with null_rate_duration Z-score >= 5 (P1 severity) — current window",
        "query": f"""
WITH stats AS (
    SELECT
        cell_tower_id,
        AVG(null_rate_duration)    AS mu,
        STDDEV(null_rate_duration) AS sigma
    FROM {CATALOG}.gold.daily_network_health
    GROUP BY cell_tower_id
)
SELECT
    h.event_date,
    h.cell_tower_id,
    ROUND(h.null_rate_duration * 100, 2)              AS null_rate_pct,
    ROUND((h.null_rate_duration - s.mu) / NULLIF(s.sigma, 0), 2) AS z_score,
    h.total_calls,
    CASE
        WHEN (h.null_rate_duration - s.mu) / NULLIF(s.sigma, 0) >= 5 THEN 'P1'
        WHEN (h.null_rate_duration - s.mu) / NULLIF(s.sigma, 0) >= 3 THEN 'P2'
        ELSE 'P3'
    END AS severity
FROM {CATALOG}.gold.daily_network_health h
JOIN stats s USING (cell_tower_id)
WHERE (h.null_rate_duration - s.mu) / NULLIF(s.sigma, 0) >= 3
ORDER BY z_score DESC, h.event_date DESC
LIMIT 50
""".strip(),
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def api(method: str, path: str, **kwargs):
    url = f"{HOST}{path}"
    resp = requests.request(method, url, headers=HEADERS, **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code} {path}: {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json()


def get_data_source_id() -> str:
    """Return the data_source_id for our warehouse (different from warehouse_id)."""
    global _DATA_SOURCE_ID
    if _DATA_SOURCE_ID:
        return _DATA_SOURCE_ID
    sources = api("GET", "/api/2.0/preview/sql/data_sources")
    for s in sources:
        if s.get("warehouse_id") == WH_ID or s.get("endpoint_id") == WH_ID:
            _DATA_SOURCE_ID = s["id"]
            return _DATA_SOURCE_ID
    raise RuntimeError(f"No data source found for warehouse {WH_ID}. Available: {sources}")


def list_queries() -> list:
    """Return all saved SQL queries (handles pagination)."""
    results, page = [], 1
    while True:
        data = api("GET", "/api/2.0/preview/sql/queries", params={"page": page, "page_size": 100})
        batch = data.get("results", [])
        results.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return results


def upsert_query(q: dict) -> str:
    """Create or update a saved query. Returns query id."""
    existing = {x["name"]: x for x in list_queries()}
    payload = {
        "name":           q["name"],
        "description":    q["description"],
        "query":          q["query"],
        "data_source_id": get_data_source_id(),
    }
    if q["name"] in existing:
        qid = existing[q["name"]]["id"]
        api("POST", f"/api/2.0/preview/sql/queries/{qid}", json=payload)
        print(f"  Updated query : {q['name']} ({qid})")
    else:
        result = api("POST", "/api/2.0/preview/sql/queries", json=payload)
        qid = result["id"]
        print(f"  Created query : {q['name']} ({qid})")
    return qid


def list_dashboards() -> list:
    data = api("GET", "/api/2.0/preview/sql/dashboards", params={"page_size": 200})
    return data.get("results", [])


# ── Lakeview (AI/BI) dashboard definition ─────────────────────────────────────
# Databricks deprecated /api/2.0/preview/sql/dashboards in favour of Lakeview.
# We build the serialized_dashboard JSON inline and POST to /api/2.0/lakeview/dashboards.

def _line(dataset: str, x_field: str, y_field: str, x_label: str, y_label: str, title: str) -> dict:
    # Lakeview v3: y must be a list
    return {
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [
                {"name": x_field, "expression": f"`{x_field}`"},
                {"name": y_field, "expression": f"`{y_field}`"},
            ],
            "disaggregated": False,
        }}],
        "spec": {
            "version": 3,
            "widgetType": "line",
            "encodings": {
                "x": {"fieldName": x_field, "scale": {"type": "temporal"}, "displayName": x_label},
                "y": [{"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label}],
            },
            "frame": {"showTitle": True, "title": title},
        },
    }


def _bar(dataset: str, x_field: str, y_field: str, x_label: str, y_label: str, title: str) -> dict:
    # Lakeview v3: y must be a list; for horizontal bar x=metric, y=category
    return {
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [
                {"name": x_field, "expression": f"`{x_field}`"},
                {"name": y_field, "expression": f"`{y_field}`"},
            ],
            "disaggregated": False,
        }}],
        "spec": {
            "version": 3,
            "widgetType": "bar",
            "encodings": {
                "x": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_label},
                "y": [{"fieldName": x_field, "scale": {"type": "ordinal"}, "displayName": x_label}],
            },
            "frame": {"showTitle": True, "title": title},
        },
    }


def _table(dataset: str, columns: list[dict], title: str) -> dict:
    """columns: [{"name": "col", "displayName": "Label"}, ...]"""
    return {
        "queries": [{"name": "main_query", "query": {
            "datasetName": dataset,
            "fields": [{"name": c["name"], "expression": f'`{c["name"]}`'} for c in columns],
            "disaggregated": False,
        }}],
        "spec": {
            "version": 3,
            "widgetType": "table",
            "encodings": {
                "columns": [
                    {"fieldName": c["name"], "displayName": c["displayName"]}
                    for c in columns
                ],
            },
            "frame": {"showTitle": True, "title": title},
        },
    }


def _widget(name: str, spec_dict: dict, x: int, y: int, w: int, h: int) -> dict:
    return {
        "widget": {"name": name, **spec_dict},
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def build_serialized_dashboard(catalog: str) -> str:
    """Return the JSON string for the Lakeview serialized_dashboard field."""
    nh_trend  = "network_health_trend"
    top_cells = "top_cells_null_rate"
    anomaly   = "anomaly_feed"
    kpi       = "kpi_trend"

    datasets = [
        {
            "name": nh_trend,
            "displayName": "Network Health Trend (30d)",
            "query": f"""
SELECT event_date,
       ROUND(AVG(null_rate_duration)*100, 2) AS null_rate_pct,
       ROUND(AVG(drop_rate_pct)*100,      2) AS drop_rate_pct,
       SUM(total_calls)                      AS total_calls
FROM {catalog}.gold.daily_network_health
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY event_date ORDER BY event_date
""".strip(),
        },
        {
            "name": top_cells,
            "displayName": "Top 10 Cells by Null Rate",
            "query": f"""
SELECT cell_tower_id,
       ROUND(AVG(null_rate_duration)*100, 2) AS avg_null_rate_pct,
       SUM(total_calls)                      AS total_calls_30d
FROM {catalog}.gold.daily_network_health
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY cell_tower_id ORDER BY avg_null_rate_pct DESC LIMIT 10
""".strip(),
        },
        {
            "name": anomaly,
            "displayName": "P1/P2 Anomaly Feed",
            "query": f"""
WITH stats AS (
  SELECT cell_tower_id,
         AVG(null_rate_duration)    AS mu,
         STDDEV(null_rate_duration) AS sigma
  FROM {catalog}.gold.daily_network_health GROUP BY cell_tower_id
)
SELECT h.event_date, h.cell_tower_id,
       ROUND(h.null_rate_duration*100, 2)                                 AS null_rate_pct,
       ROUND((h.null_rate_duration - s.mu)/NULLIF(s.sigma,0), 2)          AS z_score,
       h.total_calls,
       CASE WHEN (h.null_rate_duration-s.mu)/NULLIF(s.sigma,0) >= 5 THEN 'P1'
            WHEN (h.null_rate_duration-s.mu)/NULLIF(s.sigma,0) >= 3 THEN 'P2'
            ELSE 'P3' END AS severity
FROM {catalog}.gold.daily_network_health h
JOIN stats s USING (cell_tower_id)
WHERE (h.null_rate_duration - s.mu)/NULLIF(s.sigma, 0) >= 3
ORDER BY z_score DESC, h.event_date DESC LIMIT 50
""".strip(),
        },
        {
            "name": kpi,
            "displayName": "KPI Performance Trend (30d)",
            "query": f"""
SELECT event_date,
       ROUND(AVG(avg_dl_throughput_mbps),      2) AS dl_mbps,
       ROUND(AVG(avg_ul_throughput_mbps),      2) AS ul_mbps,
       ROUND(AVG(avg_handover_success_rate)*100,2) AS handover_pct,
       ROUND(AVG(avg_call_setup_success_rate)*100,2) AS call_setup_pct
FROM {catalog}.gold.kpi_daily_performance
WHERE event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY event_date ORDER BY event_date
""".strip(),
        },
    ]

    pages = [
        {
            "name": "network_health",
            "displayName": "Network Health",
            "layout": [
                _widget("null_rate_trend",
                        _line(nh_trend, "event_date", "null_rate_pct",
                              "Date", "Null Rate %",
                              "Daily CDR Null Rate % (30 days)"),
                        0, 0, 6, 6),
                _widget("top_cells_bar",
                        _bar(top_cells, "cell_tower_id", "avg_null_rate_pct",
                             "Cell ID", "Avg Null Rate %",
                             "Top 10 Cells by Avg Null Rate"),
                        6, 0, 6, 6),
                _widget("anomaly_table",
                        _table(anomaly, [
                            {"name": "event_date",    "displayName": "Date"},
                            {"name": "cell_tower_id",       "displayName": "Cell"},
                            {"name": "null_rate_pct", "displayName": "Null Rate %"},
                            {"name": "z_score",       "displayName": "Z-Score"},
                            {"name": "total_calls",   "displayName": "Total Calls"},
                            {"name": "severity",      "displayName": "Severity"},
                        ], "P1/P2 Anomaly Feed"),
                        0, 6, 12, 7),
            ],
        },
        {
            "name": "kpi_performance",
            "displayName": "KPI Performance",
            "layout": [
                _widget("dl_throughput",
                        _line(kpi, "event_date", "dl_mbps",
                              "Date", "DL Mbps",
                              "Avg DL Throughput (Mbps, 30 days)"),
                        0, 0, 6, 6),
                _widget("ul_throughput",
                        _line(kpi, "event_date", "ul_mbps",
                              "Date", "UL Mbps",
                              "Avg UL Throughput (Mbps, 30 days)"),
                        6, 0, 6, 6),
                _widget("handover_rate",
                        _line(kpi, "event_date", "handover_pct",
                              "Date", "Handover Success %",
                              "Handover Success Rate % (30 days)"),
                        0, 6, 6, 6),
                _widget("call_setup_rate",
                        _line(kpi, "event_date", "call_setup_pct",
                              "Date", "Call Setup Success %",
                              "Call Setup Success Rate % (30 days)"),
                        6, 6, 6, 6),
            ],
        },
    ]

    return json.dumps({"datasets": datasets, "pages": pages})


# ── Lakeview dashboard CRUD ───────────────────────────────────────────────────

def lakeview_api(method: str, path: str, **kwargs):
    """API call using Azure AD token (required for Lakeview endpoints)."""
    url = f"{HOST}{path}"
    resp = requests.request(method, url, headers=lakeview_headers(), **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code} {path}: {resp.text[:300]}")
        resp.raise_for_status()
    return resp.json() if resp.text else {}


def list_lakeview_dashboards() -> list:
    data = lakeview_api("GET", "/api/2.0/lakeview/dashboards", params={"page_size": 100})
    return data.get("dashboards", [])


def get_or_create_lakeview_dashboard(serialized: str) -> str:
    """Return existing dashboard_id or create new. Always updates serialized content."""
    for d in list_lakeview_dashboards():
        if d.get("display_name") == DASHBOARD_NAME:
            did = d["dashboard_id"]
            print(f"  Updating existing Lakeview dashboard ({did})")
            lakeview_api("PATCH", f"/api/2.0/lakeview/dashboards/{did}", json={
                "display_name":         DASHBOARD_NAME,
                "serialized_dashboard": serialized,
                "warehouse_id":         WH_ID,
            })
            return did

    result = lakeview_api("POST", "/api/2.0/lakeview/dashboards", json={
        "display_name":         DASHBOARD_NAME,
        "serialized_dashboard": serialized,
        "warehouse_id":         WH_ID,
    })
    did = result["dashboard_id"]
    print(f"  Created Lakeview dashboard ({did})")
    return did


def publish_dashboard(did: str):
    """Publish so the dashboard is accessible without edit mode."""
    try:
        lakeview_api("POST", f"/api/2.0/lakeview/dashboards/{did}/published", json={
            "warehouse_id": WH_ID,
        })
        print(f"  Published dashboard")
    except Exception as e:
        # Publishing may fail if already published or permissions differ — non-fatal
        print(f"  Publish skipped ({e})")


def build():
    print("\n=== Phase 10 — Databricks Lakeview Dashboard ===\n")

    # 1. Upsert saved SQL queries (appear in SQL Editor for ad-hoc use)
    print("Step 1: Saving SQL queries to editor…")
    for q in QUERIES:
        upsert_query(q)

    # 2. Build and create Lakeview dashboard
    print("\nStep 2: Building Lakeview dashboard definition…")
    serialized = build_serialized_dashboard(CATALOG)

    print("Step 3: Creating / updating dashboard…")
    did = get_or_create_lakeview_dashboard(serialized)

    print("Step 4: Publishing…")
    publish_dashboard(did)

    url = f"{HOST}/dashboards/{did}"
    print(f"\n=== Dashboard ready ===")
    print(f"  URL : {url}")
    print(f"\n  Open in browser and click 'Run all' to populate charts.")
    return url


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", action="store_true", help="Print dashboard URL only")
    args = parser.parse_args()

    if args.url:
        for d in list_lakeview_dashboards():
            if d.get("display_name") == DASHBOARD_NAME:
                print(f"{HOST}/dashboards/{d['dashboard_id']}")
                sys.exit(0)
        print("Dashboard not found — run without --url to create it.")
    else:
        build()
