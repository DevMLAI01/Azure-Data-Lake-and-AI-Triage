# Azure Databricks Medallion + AI Triage — Step-by-Step Build Plan

## Project Goal
Build a production-style telecom data lake on Azure using the Databricks Medallion
Architecture (Bronze → Silver → Gold) on real CDR/KPI data, with an AI triage agent
powered by Azure OpenAI. Targeted at demonstrating Data & Analytics Engineering
skills at a PwC interview.

---

## Prerequisites Checklist
> Complete these before Phase 1. Items marked 🔧 need your input/action.

- [ ] 🔧 **Azure free tier account** active at portal.azure.com
- [ ] 🔧 **Azure CLI installed** — [aka.ms/installazurecli](https://aka.ms/installazurecli)
      After install, run: `az login`
- [ ] 🔧 **Azure OpenAI access approved** — requires a separate request form at
      portal.azure.com → Azure OpenAI → Apply for access (takes 1–2 days)
- [ ] 🔧 **Databricks workspace region decided** — recommended: `eastus` or `uksouth`
- [ ] 🔧 **Slack webhook URL** (optional) — for P1 alert notifications
- [ ] **Python 3.14** ✅ already installed
- [ ] **Raw data** ✅ at `S:\My Projects\Data Lake and AI Triage System\data\raw`
- [ ] 🔧 **uv installed** — package manager used throughout this project
      Install: `pip install uv` (one-time bootstrap, then use uv for everything)

---

## Configuration

Copy `.env.example` to `.env` and fill in your values. Typical naming pattern:

```
AZURE_SUBSCRIPTION_ID=<your-subscription-id>
AZURE_REGION=eastus
RESOURCE_GROUP=rg-<your-project>-dev
ADLS_ACCOUNT=<your-adls-account>
DATABRICKS_WORKSPACE=dbw-<your-project>-dev
KEY_VAULT_NAME=kv-<your-project>-dev
AZURE_OPENAI_NAME=aoai-<your-project>-dev
FUNCTION_APP_NAME=func-<your-project>-agent-dev
DATABRICKS_TIER=Premium
ALERTS=email
```

> 💡 Secrets live in `.env` at the project root. Never commit `.env`.

---

## Phase 1 — Azure Foundation
**What:** Resource Group + ADLS Gen2 + Key Vault
**Time:** ~30 minutes
**Claude Code does:** Generates all scripts. You run them.

### Step 1.1 — Install Azure CLI + uv + Python packages ✅ IN PROGRESS
```powershell
# 1. Install Azure CLI (PowerShell as Admin)
winget install Microsoft.AzureCLI

# 2. Install uv (one-time, using pip as bootstrap only)
pip install uv

# 3. Initialise uv virtual environment for this project
#    Run from: s:\My Projects\Azure Data Lake and AI Triage System\
uv venv

# 4. Activate the venv
.venv\Scripts\activate

# 5. Install all project dependencies via uv
uv pip install -r requirements.txt
```

> From this point on, always use `uv pip install` to add packages
> and `uv run` to execute scripts. Never use bare `pip` or `python` again.

### Step 1.2 — Login and set subscription ✅ DONE
```powershell
az login
# Subscription ID already set in scripts/phase1_azure_setup.ps1
az account show   # verify correct account is active
```

### Step 1.3 — Create Resource Group
```bash
az group create \
  --name rg-<your-project>-dev \
  --location eastus \
  --tags project=telecom-triage env=dev
```

### Step 1.4 — Create ADLS Gen2
```bash
az storage account create \
  --name <your-adls-account> \
  --resource-group rg-<your-project>-dev \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \   # <-- this makes it ADLS Gen2
  --min-tls-version TLS1_2

# Create the main container
az storage fs create \
  --name telecom \
  --account-name <your-adls-account> \
  --auth-mode login
```

### Step 1.5 — Create Key Vault
```bash
az keyvault create \
  --name kv-<your-project>-dev \
  --resource-group rg-<your-project>-dev \
  --location eastus \
  --sku standard

# Store ADLS key in Key Vault
ADLS_KEY=$(az storage account keys list \
  --account-name <your-adls-account> \
  --resource-group rg-<your-project>-dev \
  --query "[0].value" -o tsv)

az keyvault secret set \
  --vault-name kv-<your-project>-dev \
  --name adls-key \
  --value "$ADLS_KEY"
```

**Checkpoint:** Browse to portal.azure.com → Resource Group and confirm: storage account + key vault visible.

---

## Phase 2 — Upload Raw Data to ADLS
**What:** Push all S-Drive raw files into `landing/` zone
**Time:** ~20 minutes (upload time depends on file sizes)
**Claude Code does:** Generates `scripts/upload_to_adls.py`. You run it once.

### Step 2.1 — Create landing folder structure
```
telecom container/
  landing/
    cdr/          ← structured/cdr/**
    customers/    ← structured/customers/**
    inventory/    ← structured/inventory/**
    kpi/          ← semi_structured/kpi/**
    snmp/         ← semi_structured/snmp/**
    mediation/    ← semi_structured/mediation/**
    app_events/   ← semi_structured/app_events/**
    noc_notes/    ← unstructured/noc_notes/**
    transcripts/  ← unstructured/transcripts/**
    syslogs/      ← unstructured/syslogs/**
    pdfs/         ← unstructured/maintenance_pdfs/**
```

### Step 2.2 — Run upload script
```powershell
uv run scripts/upload_to_adls.py
```
Script: `scripts/upload_to_adls.py` — generated in Phase 2 setup.

**Checkpoint:** In Azure portal → Storage account → Data Lake Storage → Containers
→ telecom → landing/ — all 11 folders visible with files.

---

## Phase 3 — Azure Databricks Workspace
**What:** Create workspace, cluster, mount ADLS, set up Unity Catalog
**Time:** ~45 minutes (Databricks provisioning takes ~15 min)
**Claude Code does:** Generates mount notebook and Unity Catalog setup SQL.
You do: Create workspace in portal (GUI only), create cluster.

### Step 3.1 — Create Databricks workspace (Azure Portal)
```
Portal → Create Resource → Azure Databricks →
  Workspace name:  dbw-<your-project>-dev
  Resource group:  rg-<your-project>-dev
  Region:          East US
  Pricing tier:    Standard  (not Premium — saves cost)
  
  → Review + Create → Create
  (Wait ~15 minutes for provisioning)
```

> ⚠️ Standard tier has no Unity Catalog. For Unity Catalog, you need Premium tier.
> Decision: use **Premium trial** (14-day free DBU credit covers it) OR use
> Standard and skip Unity Catalog (use database-level access instead).
> Recommendation: **Premium** — Unity Catalog is a key interview talking point.

### Step 3.2 — Create cluster (Databricks UI)
```
Databricks workspace → Compute → Create Compute →
  Policy:           Unrestricted
  Cluster mode:     Single Node       (cheapest for demo)
  Databricks Runtime: 15.4 LTS (includes Delta Lake, Python 3.11)
  Node type:        Standard_DS3_v2
  Auto-terminate:   30 minutes
  
  → Create Compute
```

### Step 3.3 — Create secret scope (links Databricks → Key Vault)
```bash
# Get Key Vault resource ID and DNS name
az keyvault show --name kv-<your-project>-dev --query "{id:id, uri:properties.vaultUri}"

# Then in Databricks:
# Go to: https://<workspace-url>#secrets/createScope
#   Scope name:       <your-secret-scope>
#   Manage principal: All Users
#   DNS name:         <Key Vault URI from above>
#   Resource ID:      <Key Vault resource ID from above>
```

### Step 3.4 — Mount ADLS (run notebook in Databricks)
Notebook: `notebooks/00_mount_adls.py` — generated in Phase 3 setup.

### Step 3.5 — Unity Catalog setup (if Premium tier)
SQL: `notebooks/00_unity_catalog_setup.sql` — generated in Phase 3 setup.
Creates: `catalog: telecon_dev` with schemas `bronze`, `silver`, `gold`.

**Checkpoint:** In Databricks → Data → telecon_dev catalog visible with 3 schemas.

---

## Phase 4 — Bronze Layer ✅ DONE
**What:** AutoLoader reads landing/ → UC managed Delta tables in bronze/
**Run via:** `uv run scripts/run_notebook.py --all-bronze`

### Notebooks (all succeeded):
| Notebook | Source format | Reads from | UC Table | Rows |
|---|---|---|---|---|
| `01_bronze_cdr.py` | Parquet | `landing/cdr/` | `bronze.cdr` | ~50,000 |
| `01_bronze_kpi.py` | JSON | `landing/kpi/` | `bronze.kpi` | ~5,000 |
| `01_bronze_snmp.py` | JSON | `landing/snmp/` | `bronze.snmp` | varies |
| `01_bronze_customers.py` | CSV | `landing/customers/` | `bronze.customers` | varies |
| `01_bronze_noc_notes.py` | binaryFile (txt) | `landing/noc_notes/` | `bronze.noc_notes` | 20 |
| `01_bronze_transcripts.py` | binaryFile (txt) | `landing/transcripts/` | `bronze.transcripts` | 50 |

### Unity Catalog implementation notes:
- All writes use `saveAsTable("telecon_dev.bronze.<table>")` — no ADLS write paths
- AutoLoader checkpoints live in UC Volume: `CREATE VOLUME IF NOT EXISTS telecon_dev.bronze.checkpoints` → `/Volumes/telecon_dev/bronze/checkpoints/<table>/`
- Used `foreachBatch` wrapper because AutoLoader streaming `.writeStream` cannot call `saveAsTable()` directly
- Used `col("_metadata.file_path")` instead of `input_file_name()` (UC restriction)
- `binaryFile` format ingests each `.txt` as one row: `(path, content, length, modificationTime)` → cast `content` to string for `raw_text`
- NOC notes: extract `incident_id`, `engineer`, `region`, `devices` via `regexp_extract` on header lines
- Transcripts: extract `call_id` (CALL-XXXXXXXX) from filename via `regexp_extract`

### Verify:
```sql
SELECT COUNT(*) FROM telecon_dev.bronze.cdr;           -- ~50,000
SELECT COUNT(*) FROM telecon_dev.bronze.noc_notes;     -- 20
SELECT COUNT(*) FROM telecon_dev.bronze.transcripts;   -- 50
DESCRIBE HISTORY telecon_dev.bronze.cdr;               -- STREAMING UPDATE
```

---

## Phase 5 — Silver Layer ✅ DONE
**What:** PII masking, quality gates, dedup, MERGE upsert
**Run via:** `uv run scripts/run_notebook.py --all-silver`

### Notebooks (all succeeded):
| Notebook | Key transforms | MERGE key |
|---|---|---|
| `02_silver_cdr.py` | SHA-256 mask MSISDNs, cast types, quarantine nulls | `cdr_id` |
| `02_silver_kpi.py` | Null checks, round rates to 4dp, quarantine nulls | `(cell_id, timestamp)` |

### Implementation notes:
- Reads from UC tables: `spark.table("telecon_dev.bronze.cdr")` — not ADLS paths
- MERGE via `DeltaTable.forName(spark, SILVER_TABLE)` (name-based, required for UC managed tables)
- First-run check: `spark.catalog.tableExists(SILVER_TABLE)` → `saveAsTable()` on first, MERGE on subsequent
- Quarantine: bad rows written to `telecon_dev.silver.quarantine` with `quarantine_reason` column
- PII masking: `SHA2(concat(msisdn, event_date), 256)` — daily salt prevents cross-day linkage; raw MSISDN columns dropped
- Column tags: `ALTER TABLE silver.cdr ALTER COLUMN caller_msisdn_hashed SET TAGS ('pii' = 'true', 'pii_type' = 'msisdn')`

### Verify:
```sql
SELECT COUNT(*) FROM telecon_dev.silver.cdr;           -- ≤ bronze (quarantined removed)
SELECT caller_msisdn_hashed FROM telecon_dev.silver.cdr LIMIT 3;  -- 64-char hex, no phone numbers
SELECT COUNT(*) FROM telecon_dev.silver.quarantine;    -- any quarantined rows
```

---

## Phase 6 — Gold Layer ✅ DONE
**What:** Business aggregates for analytics and anomaly detection
**Run via:** `uv run scripts/run_notebook.py --all-gold`

### Notebooks (all succeeded):
| Notebook | Output table | Key metrics | ZORDER |
|---|---|---|---|
| `03_gold_network_health.py` | `gold.daily_network_health` | total_calls, drop_rate_pct, null_rate_duration, avg_duration_sec | `(cell_tower_id)` |
| `03_gold_kpi_performance.py` | `gold.kpi_daily_performance` | avg throughput, handover rate, call setup rate | `(cell_id, technology)` |
| `03_gold_customer_experience.py` | `gold.customer_experience_daily` | active subscribers, avg call duration, roaming rate | none (daily only) |

### Implementation notes:
- Reads from `spark.table("telecon_dev.silver.cdr")` and `spark.table("telecon_dev.silver.kpi")`
- Writes via `saveAsTable("telecon_dev.gold.<table>")` — overwrites on each run (full recalc)
- `OPTIMIZE ... ZORDER BY` run after each write for query performance
- **Primary anomaly signal:** `null_rate_duration` = fraction of CDR rows where `duration_sec IS NULL`. Z-score ≥ 5 → P1, ≥ 3 → P2.

### Verify:
```sql
SELECT * FROM telecon_dev.gold.daily_network_health
ORDER BY event_date, null_rate_duration DESC LIMIT 10;
-- 5 days × ~100 cells; look for cells with high null_rate_duration

SELECT * FROM telecon_dev.gold.daily_network_health VERSION AS OF 1;
-- Time travel demo
```

---

## Phase 7 — Databricks Workflow (Orchestration) ✅ DONE
**What:** Wire Phases 4–6 into a single DAG with task dependencies
**Run via:** `uv run scripts/create_workflow.py`
**Manual trigger:** `uv run scripts/create_workflow.py --run`

### Workflow: `daily_medallion_pipeline`

```
Stage 1 — Bronze (6 tasks, all parallel):
  bronze_cdr  bronze_kpi  bronze_snmp  bronze_customers  bronze_noc_notes  bronze_transcripts

Stage 2 — Silver (2 tasks, each depends on ALL 6 bronze tasks):
  silver_cdr  silver_kpi

Stage 3 — Gold (3 tasks, each depends on BOTH silver tasks):
  gold_network_health  gold_kpi_performance  gold_customer_experience
```

All tasks use the existing cluster (`existing_cluster_id`). Schedule is 06:00 UTC daily, currently **PAUSED** — unpause in Databricks UI (Workflows → daily_medallion_pipeline → Schedule) when ready for production.

### Implementation notes:
- `scripts/create_workflow.py` is idempotent — deletes the job by name before recreating it
- Task keys: `bronze_cdr`, `silver_cdr`, `gold_network_health`, etc.
- `depends_on` set with `TaskDependency(task_key=...)` — Databricks enforces the DAG
- `max_concurrent_runs=1` prevents overlapping daily runs

### Verify in Databricks UI:
```
Workflows → daily_medallion_pipeline → Graph view
→ Should show 3-stage DAG: 6 bronze → 2 silver → 3 gold tasks
→ Click "Run now" to trigger a manual test run
```

---

## Phase 8 — Anomaly Detection (Databricks SQL) ✅ DONE
**What:** Z-score query on Gold tables, saved as Databricks SQL Alert
**Run via:** `uv run scripts/create_sql_alert.py`
**Preview anomaly results:** `uv run scripts/create_sql_alert.py --preview`

### Resources created:
| Resource | Name |
|---|---|
| SQL Warehouse | `telecom-sql-wh` (Serverless, 2X-Small) |
| Saved Query | `CDR Null Rate Anomaly — P1 Count` |
| Alert | `Daily CDR Null Rate P1 Alert` |

### How it works:
- **Alert query** (`ALERT_SQL`): returns a single value `p1_count` — count of cells with CDR null-rate Z-score >= 5 today
- **Alert condition**: fires when `p1_count > 0`
- **Full detail query** (`notebooks/04_anomaly_detection.sql`): returns one row per anomalous cell with `observed_null_rate`, `baseline_mean`, `z_score`, `severity` (P1/P2/P3), `total_calls`, `drop_rate_pct`
- **Z-score baseline**: rolling window using all historical days (requires >= 3 days per cell), compared against the latest available day

### Manual steps remaining (Databricks UI):
1. Open alert: Alerts tab -> "Daily CDR Null Rate P1 Alert" -> set schedule to 08:30 UTC daily
2. After Phase 9: add webhook notification -> Azure Function URL

### Notes on `p1_count = 0`:
The real CDR data does not currently produce Z-scores >= 5 at the cell level — the null rates are distributed within normal bounds. The alert will fire correctly when genuine spikes appear. For a demo, you can lower the threshold from 5 to 2 in `ALERT_SQL`.

---

## Phase 9 — AI Triage Agent ✅ DONE

**What:** Azure Function + Azure OpenAI GPT-4o agent, triggered by Databricks SQL Alert webhook

### Resources:
| Resource | Name / URL |
|---|---|
| Azure OpenAI (AI Foundry) | `aoai-<your-project>-dev` — eastus2, AIServices kind |
| OpenAI endpoint | set in `.env` as `AZURE_OPENAI_ENDPOINT` |
| GPT-4o deployment | `gpt-4o` · api_version `2024-12-01-preview` |
| Function App | `func-<your-project>-agent-dev` — Linux, Python 3.11, Consumption |
| Triage endpoint | `https://<your-function-app>.azurewebsites.net/api/triage` |

### All 4 tools verified with live data:
```
uv run scripts/test_tools.py
  [PASS]  get_anomaly_context   — queries gold.daily_network_health via SQL Warehouse
  [PASS]  get_metric_trend      — queries both gold tables for 5-day CDR + KPI history
  [PASS]  get_noc_history       — keyword search over landing/noc_notes/ in ADLS
  [PASS]  write_incident_report — writes Markdown to landing/incidents/ in ADLS
```

Agent tested end-to-end locally (`uv run scripts/test_agent_local.py`) and via live HTTP POST — incident reports confirmed written to ADLS `landing/incidents/`.

### Alert → webhook wired (via Databricks Jobs API):
The legacy `/api/2.0/preview/sql/alerts/{id}/subscriptions` endpoint is deprecated.
Webhook was wired programmatically using the Jobs API with a webhook destination and a
Databricks Job (`sql_task` type with the alert ID + destination ID). Requires Azure AD
token (not PAT) for the destinations API — PAT returns 403.

### Deploy / redeploy:
```powershell
cd azure_function
npx func azure functionapp publish <your-function-app> --python
```

### Test the triage endpoint manually:
```powershell
$key = $(az functionapp keys list --name <your-function-app> --resource-group <your-resource-group> --query "functionKeys.default" -o tsv)
Invoke-RestMethod -Method POST `
  -Uri "https://<your-function-app>.azurewebsites.net/api/triage?code=$key" `
  -ContentType "application/json" `
  -Body '{"cell_id":"CELL_0088","metric":"null_rate_duration","value":0.52,"z_score":5.8,"date":"2026-03-17","severity":"P1"}'
```

### Agent tools (4):
| Tool | Queries | Purpose |
|---|---|---|
| `get_anomaly_context` | Databricks SQL Statement API → `gold.daily_network_health` | Current metrics for flagged cell |
| `get_metric_trend` | Databricks SQL Statement API → `gold.kpi_daily_performance` | 5-day KPI history |
| `get_noc_history` | ADLS Gen2 → `landing/noc_notes/*.txt` | Past incident notes as context |
| `write_incident_report` | ADLS Gen2 → `landing/incidents/` | Save structured Markdown report |

---

## Phase 10 — Databricks Lakeview Dashboard ✅ DONE

**What:** AI/BI Lakeview dashboard with 2 pages, connected directly to Gold tables via the `telecom-sql-wh` SQL Warehouse. Created and published programmatically via the Lakeview API.

**Script:** `scripts/create_dashboard.py` (idempotent — safe to re-run)

**Dashboard URL:** `https://<your-databricks-host>/dashboards/<dashboard-id>/published`

### Pages and visuals:
| Page | Widget | Chart type | Dataset |
|---|---|---|---|
| **Network Health** | Daily CDR Null Rate % (30 days) | Line | `gold.daily_network_health` |
| **Network Health** | Top 10 Cells by Avg Null Rate | Horizontal bar | `gold.daily_network_health` |
| **Network Health** | P1/P2 Anomaly Feed | Table | `gold.daily_network_health` (Z-score >= 3) |
| **KPI Performance** | Avg DL Throughput (Mbps) | Line | `gold.kpi_daily_performance` |
| **KPI Performance** | Avg UL Throughput (Mbps) | Line | `gold.kpi_daily_performance` |
| **KPI Performance** | Handover Success Rate % | Line | `gold.kpi_daily_performance` |
| **KPI Performance** | Call Setup Success Rate % | Line | `gold.kpi_daily_performance` |

### Implementation notes:
- The legacy `/api/2.0/preview/sql/dashboards` API is fully deprecated — must use `/api/2.0/lakeview/dashboards`
- Lakeview widget specs require `"version": 3` and `y` encoding as an array (not a scalar)
- Lakeview API requires Azure AD token (`az account get-access-token`) — PAT returns 403 with "dashboards scope" error
- Update uses `PATCH`, not `PUT` (`PUT /lakeview/dashboards/{id}` returns 404)
- `daily_network_health` cell column is `cell_tower_id`; `kpi_daily_performance` uses `cell_id`
- 4 saved SQL queries also created in the SQL Editor for ad-hoc use

### Recreate / update:
```powershell
uv run scripts/create_dashboard.py        # create or update + publish
uv run scripts/create_dashboard.py --url  # print dashboard URL only
```

---

## File Structure

```
s:\My Projects\Azure Data Lake and AI Triage System\
├── PLAN.md                              ← this file
├── CLAUDE.md                            ← Claude Code guidance
├── .env.example                         ← config template (no secrets)
├── .gitignore
├── requirements.txt
├── scripts/
│   ├── upload_to_adls.py                ← Phase 2: one-time raw data upload
│   ├── test_connection.py               ← verify ADLS + Databricks connectivity
│   ├── run_notebook.py                  ← upload + run any notebook on Databricks
│   ├── create_workflow.py               ← Phase 7: Databricks Workflow DAG
│   ├── create_sql_alert.py              ← Phase 8: SQL Warehouse + anomaly alert
│   ├── test_tools.py                    ← Phase 9: verify all 4 agent tools live
│   ├── test_agent_local.py              ← Phase 9: local agent end-to-end test
│   └── create_dashboard.py              ← Phase 10: Lakeview dashboard (idempotent)
├── notebooks/
│   ├── 00_mount_adls.py                 ← Phase 3: one-time ADLS mount
│   ├── 00_unity_catalog_setup.sql       ← Phase 3: one-time catalog + schema creation
│   ├── 01_bronze_cdr.py                 ← Phase 4 ✅
│   ├── 01_bronze_kpi.py                 ← Phase 4 ✅
│   ├── 01_bronze_snmp.py                ← Phase 4 ✅
│   ├── 01_bronze_customers.py           ← Phase 4 ✅
│   ├── 01_bronze_noc_notes.py           ← Phase 4 ✅ (unstructured txt)
│   ├── 01_bronze_transcripts.py         ← Phase 4 ✅ (unstructured txt)
│   ├── 02_silver_cdr.py                 ← Phase 5 ✅
│   ├── 02_silver_kpi.py                 ← Phase 5 ✅
│   ├── 03_gold_network_health.py        ← Phase 6 ✅
│   ├── 03_gold_kpi_performance.py       ← Phase 6 ✅
│   ├── 03_gold_customer_experience.py   ← Phase 6 ✅
│   └── 04_anomaly_detection.sql         ← Phase 8 (paste into Databricks SQL)
├── azure_function/
│   ├── function_app.py                  ← Phase 9: Azure Functions entry point
│   ├── agent_runner.py                  ← Phase 9: GPT-4o tool-calling loop
│   ├── tools.py                         ← Phase 9: TOOL_DEFINITIONS + TOOL_MAP
│   ├── requirements.txt                 ← Phase 9: separate venv
│   └── host.json                        ← Phase 9
└── (no local dashboard file — dashboard lives in Databricks workspace)
```

---

## Cost Summary

| Service | Tier | Est. Monthly Cost |
|---|---|---|
| ADLS Gen2 | LRS, < 5 GB | **$0** (free 12 months) |
| Azure Key Vault | Standard | **~$0** |
| Azure Databricks | Premium trial (14-day DBU credit) | **$0 during trial** |
| Azure OpenAI GPT-4o | Pay per token | **~$1–3** (demo only) |
| Azure Functions | Consumption | **$0** (1M free/month) |
| Databricks Lakeview Dashboard | Included with workspace | **$0** |
| **Total** | | **< $5/month** |

> After Databricks trial ends: delete the workspace or switch to Standard tier.
> Job clusters only run when triggered — no idle compute cost.

---

## Progress Tracker

| Phase | Description | Status |
|---|---|---|
| 1 | Azure Foundation (RG + ADLS + Key Vault) | ✅ Done |
| 2 | Upload raw data to ADLS | ✅ Done |
| 3 | Databricks workspace + cluster + secret scope + Unity Catalog | ✅ Done |
| 4 | Bronze layer — 6 tables: cdr, kpi, snmp, customers, noc_notes, transcripts | ✅ Done |
| 5 | Silver layer — PII masking, MERGE upsert, quarantine | ✅ Done |
| 6 | Gold layer — 3 aggregation tables, OPTIMIZE + ZORDER | ✅ Done |
| 7 | Databricks Workflow (orchestration DAG) — `daily_medallion_pipeline` | ✅ Done |
| 8 | Anomaly detection SQL Alert — `CDR Null Rate P1 Alert` on SQL Warehouse | ✅ Done |
| 9 | AI Triage Agent — GPT-4o deployed, tools verified, webhook wired, incidents writing to ADLS | ✅ Done |
| 10 | Databricks Lakeview Dashboard — 2 pages, 7 charts, published | ✅ Done |

---

*Generated by Claude Code · Project: Azure Databricks Medallion + AI Triage*
