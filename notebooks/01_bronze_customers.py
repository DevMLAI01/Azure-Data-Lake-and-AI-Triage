# Databricks notebook source
# ===========================================================================
# 01_bronze_customers.py
# Phase 4 -- Bronze Layer: Customer records ingestion using AutoLoader
#
# Source : landing/customers/**/*.parquet  (ADLS, read via cluster Spark config key)
# Target : {CATALOG}.bronze.customers   (UC managed Delta table)
# Checkpt: /Volumes/{CATALOG}/bronze/checkpoints/customers/  (UC Volume)
# ===========================================================================

# COMMAND ----------

from pyspark.sql.functions import current_date, col

# CONFIGURATION — update these for your environment
ADLS_ACCOUNT = "<your-adls-account>"  # ADLS Gen2 storage account name
CATALOG      = "telecon_dev"          # Unity Catalog catalog name

BASE       = f"abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net"
SOURCE     = f"{BASE}/landing/customers/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/customers/"
TABLE      = f"{CATALOG}.bronze.customers"

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

print("Bronze customers ingestion complete")

# COMMAND ----------

count = spark.table(TABLE).count()
print(f"Rows in {TABLE}: {count:,}")
spark.sql(f"SELECT * FROM {TABLE} LIMIT 3").show(truncate=False)
