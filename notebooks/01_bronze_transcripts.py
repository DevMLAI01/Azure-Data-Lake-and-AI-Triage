# Databricks notebook source
# ===========================================================================
# 01_bronze_transcripts.py
# Phase 4 -- Bronze Layer: Customer call transcripts ingestion (unstructured text)
#
# Source : landing/transcripts/**/*.txt  (50 files, ~4KB each)
# Target : {CATALOG}.bronze.transcripts  (UC managed Delta table)
# Checkpt: /Volumes/{CATALOG}/bronze/checkpoints/transcripts/
#
# Schema:
#   call_id (extracted from filename), raw_text (full transcript),
#   file_name, file_date, ingest_date
# ===========================================================================

# COMMAND ----------

from pyspark.sql import functions as F

# CONFIGURATION — update these for your environment
ADLS_ACCOUNT = "<your-adls-account>"  # ADLS Gen2 storage account name
CATALOG      = "telecon_dev"          # Unity Catalog catalog name

BASE       = f"abfss://telecom@{ADLS_ACCOUNT}.dfs.core.windows.net"
SOURCE     = f"{BASE}/landing/transcripts/"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints/transcripts/"
TABLE      = f"{CATALOG}.bronze.transcripts"

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
            F.regexp_extract(F.col("path"), r"transcripts/(\d{4}-\d{2}-\d{2})/", 1).cast("date"))
        # Extract call_id from filename (transcript_CALL-XXXXXXXX.txt)
        .withColumn("call_id",
            F.regexp_extract(F.col("file_name"), r"(CALL-\d+)", 1))
        .withColumn("ingest_date", F.current_date())
        .withColumn("file_size_bytes", F.col("length"))
        .drop("length")
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

print("Bronze transcripts ingestion complete")

# COMMAND ----------

count = spark.table(TABLE).count()
print(f"Rows in {TABLE}: {count:,}  (expected: 50)")

spark.sql(f"""
    SELECT call_id, file_date, file_size_bytes,
           LEFT(raw_text, 200) AS text_preview
    FROM {TABLE}
    ORDER BY file_date, call_id
    LIMIT 5
""").show(truncate=False)
