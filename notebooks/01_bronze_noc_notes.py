# Databricks notebook source
# ===========================================================================
# 01_bronze_noc_notes.py
# Phase 4 -- Bronze Layer: NOC engineer notes ingestion (unstructured text)
#
# Source : landing/noc_notes/**/*.txt  (20 files, ~2KB each)
# Target : {CATALOG}.bronze.noc_notes  (UC managed Delta table)
# Checkpt: /Volumes/{CATALOG}/bronze/checkpoints/noc_notes/
#
# Schema:
#   incident_id, engineer, region, devices (extracted from header)
#   raw_text (full file content), file_name, file_date, ingest_date
# ===========================================================================

# COMMAND ----------

from pyspark.sql import functions as F

# CONFIGURATION — update these for your environment
ADLS_ACCOUNT = "<your-adls-account>"  # ADLS Gen2 storage account name
CATALOG      = "telecon_dev"          # Unity Catalog catalog name

BASE       = f"abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net"
SOURCE     = f"{BASE}/landing/noc_notes/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/noc_notes/"
TABLE      = f"{CATALOG}.bronze.noc_notes"

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
print("Volume ready")

# COMMAND ----------

def write_batch(batch_df, batch_id):
    processed = (
        batch_df
        # Cast binary content to string (UTF-8)
        .withColumn("raw_text", F.col("content").cast("string"))
        .drop("content")
        # Extract file name and date from path
        .withColumn("file_name",
            F.regexp_extract(F.col("path"), r"([^/]+)$", 1))
        .withColumn("file_date",
            F.regexp_extract(F.col("path"), r"noc_notes/(\d{4}-\d{2}-\d{2})/", 1).cast("date"))
        # Extract incident_id from filename (noc_INC-XXXXXXXX.txt)
        .withColumn("incident_id",
            F.regexp_extract(F.col("file_name"), r"(INC-\d+)", 1))
        # Extract header fields from raw_text
        .withColumn("engineer",
            F.regexp_extract(F.col("raw_text"), r"ENGINEER:\s*(.+)", 1))
        .withColumn("region",
            F.regexp_extract(F.col("raw_text"), r"REGION:\s*(.+)", 1))
        .withColumn("devices",
            F.regexp_extract(F.col("raw_text"), r"DEVICES:\s*(.+)", 1))
        .withColumn("ingest_date", F.current_date())
        # Keep path as source_file
        .withColumnRenamed("path", "source_file")
    )
    (
        processed.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TABLE)
    )

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", CHECKPOINT + "_schema")
        .load(SOURCE)
    .writeStream
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .foreachBatch(write_batch)
        .start()
        .awaitTermination()
)

print("Bronze NOC notes ingestion complete")

# COMMAND ----------

count = spark.table(TABLE).count()
print(f"Rows in {TABLE}: {count:,}  (expected: 20)")

spark.sql(f"""
    SELECT incident_id, engineer, region, file_date,
           LEFT(raw_text, 150) AS text_preview
    FROM {TABLE}
    ORDER BY file_date, incident_id
    LIMIT 5
""").show(truncate=False)
