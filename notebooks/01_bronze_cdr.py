# Databricks notebook source
# ===========================================================================
# 01_bronze_cdr.py
# Phase 4 -- Bronze Layer: CDR ingestion using AutoLoader
#
# Source : landing/cdr/**/*.parquet  (ADLS, read via cluster Spark config key)
# Target : {CATALOG}.bronze.cdr   (UC managed Delta table)
# Checkpt: /Volumes/{CATALOG}/bronze/checkpoints/cdr/  (UC Volume)
# ===========================================================================

# COMMAND ----------

from pyspark.sql.functions import current_date, col

# CONFIGURATION — update these for your environment
ADLS_ACCOUNT = "<your-adls-account>"  # ADLS Gen2 storage account name
CATALOG      = "telecon_dev"          # Unity Catalog catalog name

BASE       = f"abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net"
SOURCE     = f"{BASE}/landing/cdr/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/cdr/"
TABLE      = f"{CATALOG}.bronze.cdr"

# COMMAND ----------

# Create volume for checkpoints if it doesn't exist (UC Volume -- no external location needed)
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
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", CHECKPOINT + "_schema")
        .option("cloudFiles.inferColumnTypes", "true")
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

print("Bronze CDR ingestion complete")

# COMMAND ----------

row_count = spark.table(TABLE).count()
print(f"Rows in {TABLE}: {row_count:,}")

spark.sql(f"DESCRIBE HISTORY {TABLE}").show(5, truncate=False)
