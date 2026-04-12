# Databricks notebook source
# ===========================================================================
# 01_bronze_kpi.py
# Phase 4 -- Bronze Layer: KPI ingestion using AutoLoader
#
# Source : landing/kpi/**/*.json  (ADLS, read via cluster Spark config key)
# Target : {CATALOG}.bronze.kpi  (UC managed Delta table)
# Checkpt: /Volumes/{CATALOG}/bronze/checkpoints/kpi/  (UC Volume)
# ===========================================================================

# COMMAND ----------

from pyspark.sql.functions import current_date, col

# CONFIGURATION — update these for your environment
ADLS_ACCOUNT = "<your-adls-account>"  # ADLS Gen2 storage account name
CATALOG      = "telecon_dev"          # Unity Catalog catalog name

BASE       = f"abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net"
SOURCE     = f"{BASE}/landing/kpi/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/kpi/"
TABLE      = f"{CATALOG}.bronze.kpi"

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
print(f"Volume ready: {CATALOG}.bronze.checkpoints")

# COMMAND ----------

def write_batch(batch_df, batch_id):
    (
        batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TABLE)
    )

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", CHECKPOINT + "_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(SOURCE)
        .withColumn("ingest_date", current_date())
        .withColumn("source_file", col("_metadata.file_path"))
    .writeStream
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .foreachBatch(write_batch)
        .start()
        .awaitTermination()
)

print("Bronze KPI ingestion complete")

# COMMAND ----------

row_count = spark.table(TABLE).count()
print(f"Rows in {TABLE}: {row_count:,}")
print("  Expected: ~4,800 (120 cells x 8 files/day x 5 days)")

spark.sql(f"SELECT technology, COUNT(*) AS records FROM {TABLE} GROUP BY technology").show()
