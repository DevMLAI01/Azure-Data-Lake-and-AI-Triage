# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Production-style Azure telecom data lake built for a PwC Data & Analytics Engineering Manager interview. Demonstrates Databricks Medallion Architecture (Bronze → Silver → Gold) on real CDR/KPI data, with an Azure OpenAI-powered AI triage agent.

Raw source data lives at `S:\My Projects\Data Lake and AI Triage System\data\raw` — it is never regenerated; the upload script moves it to ADLS once.

## Package Management

Always use `uv`. Never use bare `pip` or `python` to run scripts.

```powershell
# Activate venv (Windows PowerShell)
.venv\Scripts\activate

# Add a package
uv pip install <package>

# Run a script
uv run scripts/<script>.py

# Sync from requirements.txt
uv pip install -r requirements.txt
```

The venv is at `.venv/` (Python 3.13). The Azure Function has its own separate `azure_function/requirements.txt` and should be installed into a separate venv under `azure_function/`.

## Running Scripts

```powershell
# Upload raw data to ADLS (run once after Phase 1)
uv run scripts/upload_to_adls.py

# Verify ADLS + Databricks connectivity
uv run scripts/test_connection.py

# Run a single notebook on Databricks
uv run scripts/run_notebook.py --notebook 01_bronze_cdr.py

# Run all notebooks in a layer
uv run scripts/run_notebook.py --all-bronze
uv run scripts/run_notebook.py --all-silver
uv run scripts/run_notebook.py --all-gold

# Run the full pipeline (bronze → silver → gold)
uv run scripts/run_notebook.py --full-pipeline

# Create/recreate the Databricks Workflow DAG (idempotent)
uv run scripts/create_workflow.py
uv run scripts/create_workflow.py --run   # also trigger a manual run

# Create SQL Warehouse + saved query + alert (idempotent)
uv run scripts/create_sql_alert.py
uv run scripts/create_sql_alert.py --preview  # run the anomaly query and print results

# Verify all 4 agent tools against live Azure resources
uv run scripts/test_tools.py

# Test the AI agent locally (requires .env filled in)
uv run scripts/test_agent_local.py

# Create/update Databricks Lakeview dashboard (Phase 10, idempotent)
uv run scripts/create_dashboard.py
uv run scripts/create_dashboard.py --url   # print existing dashboard URL
```

`scripts/create_dashboard.py` creates a Databricks Lakeview (AI/BI) dashboard with two pages: Network Health (null rate trend, top 10 cells bar, anomaly feed table) and KPI Performance (DL/UL throughput, handover/call-setup success rates). The dashboard uses inline SQL datasets pointing at the Gold tables. It requires an Azure AD token for the Lakeview API (PAT tokens lack the `dashboards` scope); the script fetches one automatically via `az account get-access-token`.

`scripts/run_notebook.py` uploads each notebook to the Databricks workspace (`/telecom-triage/`), submits a one-time job run on the running cluster, and polls until completion. It prints live status and captures error output on failure. Terminal states checked: `TERMINATED`, `SKIPPED`, `INTERNAL_ERROR`.

All scripts load secrets from `.env` via `python-dotenv`. Copy `.env.example` → `.env` and fill in secrets before running anything.

**Required `.env` variables** (see `.env.example` for all defaults):
- `ADLS_ACCOUNT_NAME`, `ADLS_KEY`, `ADLS_CONTAINER`
- `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`, `DATABRICKS_CATALOG`
- `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_KEY`, `AZURE_OPENAI_GPT4O_DEPLOYMENT`

## Azure Function — Deploy & Test

The Function App uses a separate venv. Set it up once:
```powershell
cd azure_function
uv venv
uv pip install -r requirements.txt
```

Deploy or redeploy to Azure:
```powershell
cd azure_function
npx func azure functionapp publish <your-function-app> --python
```

Retrieve the function key (required for all requests):
```bash
az functionapp keys list --name <your-function-app> \
  --resource-group <your-resource-group> --query "functionKeys.default" -o tsv
```

Test the triage endpoint manually:
```powershell
$key = $(az functionapp keys list --name <your-function-app> --resource-group <your-resource-group> --query "functionKeys.default" -o tsv)
Invoke-RestMethod -Method POST `
  -Uri "https://<your-function-app>.azurewebsites.net/api/triage?code=$key" `
  -ContentType "application/json" `
  -Body '{"cell_id":"CELL_0088","metric":"null_rate_duration","value":0.52,"z_score":5.8,"date":"2026-03-17","severity":"P1"}'
```

## Azure Resource Names

All resource names are configured via `.env` — see `.env.example` for the full list. Typical naming pattern:

| Resource | Example name |
|---|---|
| Subscription | set in `.env` as `AZURE_SUBSCRIPTION_ID` |
| Resource Group | `rg-<project>-dev` |
| ADLS Gen2 | `<your-adls-account>` · container: `telecom` |
| Key Vault | `kv-<project>-dev` · secret: `adls-key` |
| Databricks | `dbw-<project>-dev` · catalog: `telecon_dev` |
| Azure OpenAI | `aoai-<project>-dev` |
| Function App | `func-triage-agent-dev` |

## Architecture

```
S-Drive raw data
  → upload_to_adls.py
    → ADLS landing/          (raw, never transformed)
      → Databricks AutoLoader
        → bronze/            (Delta, append-only, ingest_date partition)
          → Silver notebooks (PII mask, MERGE upsert, quarantine)
            → silver/        (Delta, event_date partition)
              → Gold notebooks (aggregations)
                → gold/      (Delta, OPTIMIZE + ZORDER)
                  → Databricks SQL Alert (Z-score anomaly)
                    → Azure Function HTTP trigger
                      → OpenAI GPT-4o agent loop
                        → tools query live Gold tables + noc_notes
                          → ADLS landing/incidents/*.md
```

## Notebooks (Databricks)

Files in `notebooks/` are Databricks notebooks in `.py` format — import them via Databricks UI (Workspace → Import). They use the `# COMMAND ----------` separator and `# MAGIC %md` for markdown cells.

**Execution order is strict:**
1. `00_mount_adls.py` — one-time ADLS config, requires `telecom-scope` secret scope linked to Key Vault
2. `00_unity_catalog_setup.sql` — one-time, run in Databricks SQL Editor
3. `01_bronze_*.py` — AutoLoader ingestion, can run in parallel (6 notebooks: cdr, kpi, snmp, customers, noc_notes, transcripts)
4. `02_silver_cdr.py` → `02_silver_kpi.py` — must run after bronze
5. `03_gold_*.py` — must run after silver
6. `04_anomaly_detection.sql` — run in Databricks SQL Editor, save as Alert

All notebooks have a `ADLS_ACCOUNT` config variable at the top — set it to your ADLS Gen2 account name. They also have a `CATALOG` variable for the Unity Catalog catalog name (default: `telecon_dev`). The base path pattern is `abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net`.

**Unity Catalog constraints (all notebooks comply):**
- **No External Location** — reads from ADLS landing/ work via cluster Spark config key; writes use UC managed tables only (`saveAsTable()`), never raw ADLS paths
- **DBFS root disabled** — AutoLoader checkpoints use UC Volumes: `CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints`, then path `/Volumes/{CATALOG}/bronze/checkpoints/<table>/`
- **`_metadata.file_path`** instead of `input_file_name()` — UC does not allow `input_file_name()`
- **`foreachBatch` pattern** — AutoLoader streaming writes use a `foreachBatch` wrapper so that each micro-batch can call `.write.saveAsTable()` (streaming `.writeStream` cannot call `saveAsTable()` directly)
- **`DeltaTable.forName()`** — UC managed tables must use name-based, not path-based, Delta references

**Key Delta Lake patterns used:**
- AutoLoader with `cloudFiles.format` + `trigger(availableNow=True)` — batch mode, exactly-once
- `binaryFile` format for unstructured text — reads `.txt` files as `(path, content, length, modificationTime)`, cast `content` to string to get `raw_text`
- `MERGE` upsert on natural key (e.g. `cdr_id`) for idempotency in silver
- Quarantine: bad rows written to `telecon_dev.silver.quarantine` table with `quarantine_reason` column
- `OPTIMIZE ... ZORDER BY (cell_id)` runs after every gold write

## AI Agent (azure_function/)

The agent is an Azure Function HTTP trigger (`POST /api/triage`). It receives an anomaly JSON payload from a Databricks SQL Alert webhook and runs a GPT-4o tool-calling loop.

**Tool call sequence the agent follows:**
1. `get_anomaly_context(cell_id, event_date)` — queries `gold.daily_network_health` via Databricks Statement Execution API
2. `get_metric_trend(cell_id)` — queries both gold tables for 5-day history
3. `get_noc_history(keyword)` — keyword search over `landing/noc_notes/*.txt` in ADLS
4. `write_incident_report(...)` — writes structured Markdown to `landing/incidents/`

Tool definitions live in `tools.py` (`TOOL_DEFINITIONS` list + `TOOL_MAP` dict). `agent_runner.py` owns the message loop. `function_app.py` is the Azure Functions entry point only — no business logic there.

The agent has a `max_turns = 10` safety guard. Local testing uses `scripts/test_agent_local.py` which calls `run_agent()` directly without deploying.

## Key Data Schemas

**CDR** (real columns from source parquet):
`cdr_id, caller_msisdn, callee_msisdn, call_type, start_timestamp, duration_sec, cell_tower_id, disconnect_reason, roaming_flag, data_volume_mb`

Silver masks both MSISDN columns → `caller_msisdn_hashed`, `callee_msisdn_hashed` using `SHA2(concat(msisdn, event_date), 256)`. Raw columns are dropped.

**KPI** (real columns from source JSON):
`cell_id, technology, timestamp, dl_throughput_mbps, ul_throughput_mbps, rrc_connected_users, handover_success_rate, call_setup_success_rate, dl_prb_utilization, interference_level_dbm`

**Primary anomaly signal:** `null_rate_duration` in `gold.daily_network_health` — fraction of CDRs with null `duration_sec`. Z-score ≥ 5 → P1, ≥ 3 → P2.

## Key Data Schemas — Unstructured (Bronze only)

**NOC Notes** (`telecon_dev.bronze.noc_notes`, 20 rows):
`incident_id (INC-XXXXXXXX), engineer, region, devices, raw_text, file_name, file_date, ingest_date, source_file`
- `incident_id`, `engineer`, `region`, `devices` extracted via `regexp_extract` from file header
- One row per `.txt` file from `landing/noc_notes/YYYY-MM-DD/noc_INC-XXXXXXXX.txt`

**Call Transcripts** (`telecon_dev.bronze.transcripts`, 50 rows):
`call_id (CALL-XXXXXXXX), raw_text, file_name, file_date, file_size_bytes, ingest_date, source_file`
- `call_id` extracted from filename `transcript_CALL-XXXXXXXX.txt`
- One row per `.txt` file from `landing/transcripts/YYYY-MM-DD/`
- Full transcript text preserved in `raw_text` for downstream NLP / AI agent context

## Build Progress

See `PLAN.md` for the full phase-by-phase plan and current progress tracker.

**Completed:** Phases 1–9 (Azure foundation → Bronze → Silver → Gold → Workflow DAG → SQL Alert → AI Triage Agent deployed)

**Completed:** All 10 phases done. Function App deployed, all 4 agent tools verified against live data, Databricks Lakeview dashboard live. See `PLAN.md` for full details.
