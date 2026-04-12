# Databricks notebook source
# ===========================================================================
# 03_gold_customer_experience.py
# Phase 6 -- Gold Layer: Daily customer experience aggregates
#
# Source : {CATALOG}.silver.cdr  (UC managed table)
# Target : {CATALOG}.gold.customer_experience_daily  (UC managed table)
# ===========================================================================

# COMMAND ----------

from pyspark.sql import functions as F

# CONFIGURATION — update these for your environment
CATALOG = "telecon_dev"  # Unity Catalog catalog name

SILVER_TABLE = f"{CATALOG}.silver.cdr"
GOLD_TABLE   = f"{CATALOG}.gold.customer_experience_daily"

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)

gold_df = (
    silver_df
    .groupBy("event_date")
    .agg(
        F.countDistinct("caller_msisdn_hashed").alias("active_subscribers"),
        F.count("cdr_id")                      .alias("total_calls"),
        F.round(F.avg("duration_sec"), 1)       .alias("avg_call_duration_sec"),
        F.round(
            F.sum((F.col("disconnect_reason") == "CONGESTION").cast("int"))
            / F.count("cdr_id") * 100, 2)       .alias("network_drop_rate_pct"),
        F.sum(F.col("roaming_flag").cast("int")).alias("roaming_calls"),
        F.round(F.avg("data_volume_mb"), 3)     .alias("avg_data_volume_mb"),
        F.sum((F.col("call_type") == "VOICE").cast("int")).alias("voice_calls"),
        F.sum((F.col("call_type") == "DATA").cast("int")) .alias("data_sessions"),
        F.sum((F.col("call_type") == "SMS").cast("int"))  .alias("sms_count"),
    )
)

(
    gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TABLE)
)

print("Gold customer experience written")

# COMMAND ----------

spark.sql(f"""
    SELECT event_date, active_subscribers, total_calls,
           avg_call_duration_sec, network_drop_rate_pct
    FROM {GOLD_TABLE}
    ORDER BY event_date
""").show()
