# Databricks notebook source
# ===========================================================================
# 03_gold_kpi_performance.py
# Phase 6 -- Gold Layer: Daily KPI performance aggregates
#
# Source : {CATALOG}.silver.kpi  (UC managed table)
# Target : {CATALOG}.gold.kpi_daily_performance  (UC managed table)
#
# Output columns:
#   event_date, cell_id, technology,
#   avg_dl_throughput_mbps, avg_ul_throughput_mbps,
#   avg_handover_success_rate, avg_call_setup_success_rate,
#   avg_prb_utilization, avg_interference_dbm,
#   peak_connected_users, min_call_setup_success_rate
# ===========================================================================

# COMMAND ----------

from pyspark.sql import functions as F

# CONFIGURATION — update these for your environment
CATALOG = "telecon_dev"  # Unity Catalog catalog name

SILVER_TABLE = f"{CATALOG}.silver.kpi"
GOLD_TABLE   = f"{CATALOG}.gold.kpi_daily_performance"

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE)

gold_df = (
    silver_df
    .groupBy("event_date", "cell_id", "technology")
    .agg(
        F.round(F.avg("dl_throughput_mbps"),  2).alias("avg_dl_throughput_mbps"),
        F.round(F.avg("ul_throughput_mbps"),  2).alias("avg_ul_throughput_mbps"),
        F.round(F.avg("handover_success_rate"),   4).alias("avg_handover_success_rate"),
        F.round(F.avg("call_setup_success_rate"), 4).alias("avg_call_setup_success_rate"),
        F.round(F.min("call_setup_success_rate"), 4).alias("min_call_setup_success_rate"),
        F.round(F.avg("dl_prb_utilization"),  4).alias("avg_prb_utilization"),
        F.round(F.avg("interference_level_dbm"), 2).alias("avg_interference_dbm"),
        F.max("rrc_connected_users").alias("peak_connected_users"),
    )
)

(
    gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .saveAsTable(GOLD_TABLE)
)

print("Gold KPI daily performance written")
print(f"  Rows: {gold_df.count():,}")

# COMMAND ----------

spark.sql(f"OPTIMIZE {GOLD_TABLE} ZORDER BY (cell_id, technology)")

# COMMAND ----------

# Show cells with lowest call setup success rate (potential P1 signal)
spark.sql(f"""
    SELECT
        event_date, cell_id, technology,
        avg_call_setup_success_rate,
        min_call_setup_success_rate,
        avg_dl_throughput_mbps
    FROM {GOLD_TABLE}
    ORDER BY avg_call_setup_success_rate ASC
    LIMIT 10
""").show(truncate=False)
