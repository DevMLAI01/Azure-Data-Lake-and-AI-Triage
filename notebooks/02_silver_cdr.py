# Databricks notebook source
# ===========================================================================
# 02_silver_cdr.py
# Phase 5 -- Silver Layer: CDR cleaning, PII masking, MERGE upsert
#
# Source    : {CATALOG}.bronze.cdr  (UC managed table)
# Target    : {CATALOG}.silver.cdr  (UC managed table)
# Quarantine: {CATALOG}.quarantine.cdr  (UC managed table)
#
# Transforms:
#   1. Quality gate -- reject rows missing cdr_id, caller_msisdn, start_timestamp
#   2. PII masking  -- SHA-256(msisdn + event_date) on caller + callee
#   3. Type casting -- duration_sec -> int, data_volume_mb -> double
#   4. Dedup        -- on cdr_id (idempotent re-runs)
#   5. MERGE upsert -- safe to re-run without creating duplicates
# ===========================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver CDR -- PII Masking + Quality Gate + MERGE

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# CONFIGURATION — update these for your environment
CATALOG = "telecon_dev"  # Unity Catalog catalog name

BRONZE_TABLE     = f"{CATALOG}.bronze.cdr"
SILVER_TABLE     = f"{CATALOG}.silver.cdr"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.cdr"

# COMMAND ----------

# MAGIC %md ### Step 1 -- Read bronze and split good/bad rows

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
print(f"Bronze row count: {bronze_df.count():,}")

# Quality rules: mandatory fields must not be null
good_df = bronze_df.filter(
    F.col("cdr_id").isNotNull()
    & F.col("caller_msisdn").isNotNull()
    & F.col("start_timestamp").isNotNull()
    & F.col("cell_tower_id").isNotNull()
)

bad_df = bronze_df.subtract(good_df)
print(f"  Passed quality gate : {good_df.count():,}")
print(f"  Quarantined         : {bad_df.count():,}")

# COMMAND ----------

# MAGIC %md ### Step 2 -- Quarantine bad rows

# COMMAND ----------

if bad_df.count() > 0:
    (
        bad_df
            .withColumn("quarantine_reason", F.lit("null_mandatory_field"))
            .withColumn("quarantine_ts",     F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .saveAsTable(QUARANTINE_TABLE)
    )
    print(f"  {bad_df.count():,} rows written to quarantine")

# COMMAND ----------

# MAGIC %md ### Step 3 -- PII masking + type casting

# COMMAND ----------

# Daily salt = event date string -- deterministic joins within a day,
# non-reversible across days without the original value
event_date_salt = F.date_format(
    F.to_date(F.col("start_timestamp")), "yyyy-MM-dd"
)

silver_df = (
    good_df
    # SHA-256 PII masking on both MSISDNs
    .withColumn("caller_msisdn_hashed",
        F.sha2(F.concat(F.col("caller_msisdn"), event_date_salt), 256))
    .withColumn("callee_msisdn_hashed",
        F.sha2(F.concat(F.col("callee_msisdn"), event_date_salt), 256))
    .drop("caller_msisdn", "callee_msisdn")    # remove raw PII
    # Derived columns
    .withColumn("event_date", F.to_date(F.col("start_timestamp")))
    .withColumn("hour",       F.hour(F.col("start_timestamp").cast("timestamp")))
    # Type casting
    .withColumn("duration_sec",    F.col("duration_sec").cast("integer"))
    .withColumn("data_volume_mb",  F.col("data_volume_mb").cast("double"))
    .withColumn("roaming_flag",    F.col("roaming_flag").cast("boolean"))
    # Dedup within batch
    .dropDuplicates(["cdr_id"])
)

print(f"Silver rows (after masking + dedup): {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md ### Step 4 -- MERGE upsert (idempotent)

# COMMAND ----------

# Check if silver table already exists
silver_table_exists = spark.catalog.tableExists(SILVER_TABLE)

if silver_table_exists:
    print("MERGE into existing silver.cdr table...")
    (
        DeltaTable.forName(spark, SILVER_TABLE).alias("target")
        .merge(
            silver_df.alias("source"),
            "target.cdr_id = source.cdr_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    print("First run -- writing silver.cdr as new managed table...")
    (
        silver_df.write
            .format("delta")
            .partitionBy("event_date")
            .saveAsTable(SILVER_TABLE)
    )

print("Silver CDR write complete")

# COMMAND ----------

# MAGIC %md ### Step 5 -- Tag PII columns in Unity Catalog

# COMMAND ----------

# Tag pseudonymised columns (Unity Catalog column-level tagging)
spark.sql(f"""
    ALTER TABLE {SILVER_TABLE}
    ALTER COLUMN caller_msisdn_hashed
    SET TAGS ('pii' = 'pseudonymised', 'original_field' = 'caller_msisdn')
""")
spark.sql(f"""
    ALTER TABLE {SILVER_TABLE}
    ALTER COLUMN callee_msisdn_hashed
    SET TAGS ('pii' = 'pseudonymised', 'original_field' = 'callee_msisdn')
""")

print("Unity Catalog PII tags applied")

# COMMAND ----------

# MAGIC %md ### Verify

# COMMAND ----------

spark.sql(f"""
    SELECT
        event_date,
        COUNT(*)                                              AS total_calls,
        SUM(CASE WHEN duration_sec IS NULL THEN 1 ELSE 0 END) AS null_duration,
        ROUND(AVG(duration_sec), 1)                           AS avg_duration_sec
    FROM {SILVER_TABLE}
    GROUP BY event_date
    ORDER BY event_date
""").show()
