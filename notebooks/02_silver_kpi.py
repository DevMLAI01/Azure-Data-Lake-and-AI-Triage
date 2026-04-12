# Databricks notebook source
# ===========================================================================
# 02_silver_kpi.py
# Phase 5 -- Silver Layer: KPI cleaning and dedup
#
# Source : {CATALOG}.bronze.kpi  (UC managed table)
# Target : {CATALOG}.silver.kpi  (UC managed table)
#
# Transforms:
#   1. Null checks on cell_id and timestamp
#   2. Round rates to 4 decimal places
#   3. Dedup on (cell_id, timestamp)
#   4. Add event_date and hour columns
#   5. MERGE upsert
# ===========================================================================

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# CONFIGURATION — update these for your environment
CATALOG = "telecon_dev"  # Unity Catalog catalog name

BRONZE_TABLE     = f"{CATALOG}.bronze.kpi"
SILVER_TABLE     = f"{CATALOG}.silver.kpi"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.kpi"

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)
print(f"Bronze KPI rows: {bronze_df.count():,}")

# Quality gate
good_df = bronze_df.filter(
    F.col("cell_id").isNotNull()
    & F.col("timestamp").isNotNull()
    & F.col("technology").isNotNull()
)

bad_df = bronze_df.subtract(good_df)
if bad_df.count() > 0:
    (
        bad_df.withColumn("quarantine_reason", F.lit("null_mandatory_field"))
            .write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)
    )
    print(f"  {bad_df.count():,} rows quarantined")

# COMMAND ----------

silver_df = (
    good_df
    .withColumn("event_date", F.to_date("timestamp"))
    .withColumn("hour",       F.hour(F.col("timestamp").cast("timestamp")))
    # Round percentages/rates for consistency
    .withColumn("handover_success_rate",    F.round("handover_success_rate", 4))
    .withColumn("call_setup_success_rate",  F.round("call_setup_success_rate", 4))
    .withColumn("dl_prb_utilization",       F.round("dl_prb_utilization", 4))
    .withColumn("dl_throughput_mbps",       F.round("dl_throughput_mbps", 2))
    .withColumn("ul_throughput_mbps",       F.round("ul_throughput_mbps", 2))
    .dropDuplicates(["cell_id", "timestamp"])
)

print(f"Silver KPI rows: {silver_df.count():,}")

# COMMAND ----------

silver_table_exists = spark.catalog.tableExists(SILVER_TABLE)

if silver_table_exists:
    (
        DeltaTable.forName(spark, SILVER_TABLE).alias("t")
        .merge(
            silver_df.alias("s"),
            "t.cell_id = s.cell_id AND t.timestamp = s.timestamp"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").partitionBy("event_date").saveAsTable(SILVER_TABLE)

print("Silver KPI write complete")

# COMMAND ----------

spark.sql(f"""
    SELECT technology, event_date, COUNT(*) AS readings,
           ROUND(AVG(handover_success_rate),4) AS avg_handover,
           ROUND(AVG(call_setup_success_rate),4) AS avg_cssr
    FROM {SILVER_TABLE}
    GROUP BY technology, event_date
    ORDER BY technology, event_date
""").show()
