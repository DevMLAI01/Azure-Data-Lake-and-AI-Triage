# Databricks notebook source
# ===========================================================================
# 00_mount_adls.py
# Phase 3 — Mount ADLS Gen2 and configure Spark for the workspace
#
# Run this ONCE after creating the Databricks workspace.
# Prerequisite: secret scope linked to Key Vault (see PLAN.md Phase 3.3)
#               with secret "adls-key" already stored.
#
# CONFIGURATION — update these for your environment:
SECRET_SCOPE = "<your-secret-scope>"   # Databricks secret scope name (e.g. "telecom-scope")
ADLS_ACCOUNT = "<your-adls-account>"  # ADLS Gen2 storage account name
# ===========================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Verify secret scope is working

# COMMAND ----------

# Test that secret scope is accessible
try:
    key = dbutils.secrets.get(scope=SECRET_SCOPE, key="adls-key")
    print("✓ Secret scope working — key retrieved (hidden for security)")
except Exception as e:
    print(f"✗ Secret scope error: {e}")
    print("  Go to: https://<workspace>#secrets/createScope")
    print("  Create a scope linked to your Key Vault (see PLAN.md Phase 3.3)")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Configure Spark to access ADLS Gen2

# COMMAND ----------

ADLS_KEY     = dbutils.secrets.get(scope=SECRET_SCOPE, key="adls-key")

spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    ADLS_KEY
)

BASE = f"abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net"
print(f"✓ Spark configured for ADLS: {BASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Verify landing zone files are visible

# COMMAND ----------

landing_folders = [
    "landing/cdr", "landing/kpi", "landing/snmp",
    "landing/customers", "landing/noc_notes", "landing/pdfs"
]

print(f"{'Folder':<35} {'Files':>6}")
print("-" * 42)
for folder in landing_folders:
    try:
        files = dbutils.fs.ls(f"{BASE}/{folder}")
        # Count recursively (folder may have date subfolders)
        all_files = []
        for item in files:
            if item.isDir():
                all_files.extend(dbutils.fs.ls(item.path))
            else:
                all_files.append(item)
        print(f"  {folder:<33} {len(all_files):>6}")
    except Exception:
        print(f"  {folder:<33} {'NOT FOUND':>6}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Store config as notebook widgets (reusable across notebooks)

# COMMAND ----------

# Save BASE path as a cluster-level variable so all notebooks can reference it
spark.conf.set("telecom.adls.base", BASE)
spark.conf.set("telecom.adls.account", ADLS_ACCOUNT)

print("✓ Spark config set:")
print(f"  telecom.adls.base    = {spark.conf.get('telecom.adls.base')}")
print(f"  telecom.adls.account = {spark.conf.get('telecom.adls.account')}")
print("\nSetup complete. Proceed to 01_bronze_*.py notebooks.")
