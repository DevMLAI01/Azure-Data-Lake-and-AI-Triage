# Databricks notebook source
# ===========================================================================
# 03_gold_network_health.py
# Phase 6 -- Gold Layer: Daily network health aggregates from Silver CDR
#
# Source : {CATALOG}.silver.cdr  (UC managed table)
# Target : {CATALOG}.gold.daily_network_health  (UC managed table)
#
# Output columns:
#   event_date, cell_tower_id,
#   total_calls, congestion_drops, drop_rate_pct,
#   avg_duration_sec, null_rate_duration,
#   roaming_calls, avg_data_volume_mb
# ===========================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold -- Daily Network Health
# MAGIC Aggregates CDR data per cell per day. Primary table for:
# MAGIC - Power BI network health dashboard
# MAGIC - Anomaly detection Z-score query
# MAGIC - AI triage agent context

# COMMAND ----------

from pyspark.sql import functions as F

# CONFIGURATION — update these for your environment
CATALOG = "telecon_dev"  # Unity Catalog catalog name

SILVER_TABLE = f"{CATALOG}.silver.cdr"
GOLD_TABLE   = f"{CATALOG}.gold.daily_network_health"

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)

gold_df = (
    silver_df
    .groupBy("event_date", "cell_tower_id")
    .agg(
        F.count("cdr_id")
            .alias("total_calls"),
        F.sum((F.col("disconnect_reason") == "CONGESTION").cast("int"))
            .alias("congestion_drops"),
        F.round(
            F.sum((F.col("disconnect_reason") == "CONGESTION").cast("int"))
            / F.count("cdr_id") * 100, 2)
            .alias("drop_rate_pct"),
        F.round(F.avg("duration_sec"), 1)
            .alias("avg_duration_sec"),
        F.round(
            F.sum(F.col("duration_sec").isNull().cast("int"))
            / F.count("cdr_id"), 4)
            .alias("null_rate_duration"),        # key anomaly signal
        F.sum(F.col("roaming_flag").cast("int"))
            .alias("roaming_calls"),
        F.round(F.avg("data_volume_mb"), 3)
            .alias("avg_data_volume_mb"),
        F.countDistinct("call_type")
            .alias("distinct_call_types"),
    )
)

print(f"Gold network health rows: {gold_df.count():,}")

# COMMAND ----------

(
    gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .saveAsTable(GOLD_TABLE)
)

print("Gold daily_network_health written")

# COMMAND ----------

spark.sql(f"OPTIMIZE {GOLD_TABLE} ZORDER BY (cell_tower_id)")
print("OPTIMIZE + ZORDER complete")

# COMMAND ----------

# MAGIC %md ### Verify -- top cells by null rate (anomaly preview)

# COMMAND ----------

spark.sql(f"""
    SELECT
        event_date,
        cell_tower_id,
        total_calls,
        null_rate_duration,
        drop_rate_pct,
        avg_duration_sec
    FROM {GOLD_TABLE}
    ORDER BY null_rate_duration DESC
    LIMIT 10
""").show(truncate=False)
