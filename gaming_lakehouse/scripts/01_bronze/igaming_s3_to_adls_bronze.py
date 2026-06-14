import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp, lit

# 1. Capture parameters passed from the Databricks Workflow Task
dbutils.widgets.text("country_code", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("catalog", "main")

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--country_code")
parser.add_argument("--table_name")
parser.add_argument("--catalog")
args, _ = parser.parse_known_args(sys.argv[1:])

country = (args.country_code or dbutils.widgets.get("country_code") or "").strip()  # e.g., "de", "at", "dk"
table = (args.table_name or dbutils.widgets.get("table_name") or "").strip()  # e.g., "gametransaction", "wallet"
catalog = (args.catalog or dbutils.widgets.get("catalog") or "main").strip()


if not table:
    raise ValueError("Missing critical task parameter: 'table_name'.")

if not country:
    raise ValueError("Missing critical task parameter: 'country_code'.")

# 2. Authenticate against AWS S3 securely using Databricks Secret Scopes
# NOTE: Commented out for POC on Databricks free trial (no S3 access)
# aws_access_key = dbutils.secrets.get(scope="source_system_scope", key="aws_access_key")
# aws_secret_key = dbutils.secrets.get(scope="source_system_scope", key="aws_secret_key")
# spark.conf.set("fs.s3a.access.key", aws_access_key)
# spark.conf.set("fs.s3a.secret.key", aws_secret_key)
# spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.threshold", "32")

# 3. Formulate Ingestion Paths (Unity Catalog Volumes — POC on Databricks free trial)
# Production example: s3a://<source-bucket>/{country}/parquet/{table}/
source_path = f"/Volumes/{catalog}/bronze/raw_landing/{country}/{table}/"
checkpoint_path = f"/Volumes/{catalog}/bronze/raw_landing/_bronze_checkpoints/{country}/{table}/"
schema_evolution_path = f"/Volumes/{catalog}/bronze/raw_landing/_schemas/{country}/{table}/"

# 4. Guard: Auto Loader fails with CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE if the source folder is
# empty and no persisted schema exists yet. Exit cleanly instead of crashing the job task.
try:
    source_files = dbutils.fs.ls(source_path)
    parquet_files = [f for f in source_files if f.name.endswith(".parquet")]
except Exception:
    parquet_files = []

if not parquet_files:
    print(f"[AutoLoader] country={country} table={table}")
    print(f"[AutoLoader] Source folder is empty or has no parquet files — nothing to ingest.")
    print(f"[AutoLoader] Upload files to {source_path} and re-run.")
    raise SystemExit(0)

# 5. Stream incoming data using Auto Loader with Schema Inference
raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_evolution_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
)

# 6. Inject Lineage Audit Columns using explicitly imported functions
enriched_stream = (
    raw_stream.withColumn("country_code", lit(country))
    .withColumn("input_file_name", col("_metadata.file_path"))
    .withColumn("bronze_inserted_at", current_timestamp())
)

# 7. Commit the incremental batch directly into the unified Delta Target Table
query = (
    enriched_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Runs caught-up data then stops to save Serverless costs
    .toTable(f"{catalog}.bronze.{table}")
)

# 8. Force script to block until the streaming write thread finishes committing
# Auto Loader idempotency: the checkpoint records every file path it has already ingested.
# On re-runs with no new files, it processes 0 rows and exits cleanly — no duplicates written.
run_start_time = datetime.now(timezone.utc)
query.awaitTermination()

# 9. Post-run diagnostics
# numInputFiles in lastProgress is unreliable for cloudFiles (Auto Loader) — it tracks files via
# its own internal registry, not Spark streaming metrics. Instead, sum numInputRows across all
# micro-batches in recentProgress, and count distinct input_file_name from the bronze table
# (written this run) for an accurate file count.
total_rows = sum(p.get("numInputRows", 0) for p in query.recentProgress)

if total_rows > 0:
    run_start_str = run_start_time.strftime("%Y-%m-%d %H:%M:%S")
    distinct_files_df = spark.sql(f"""
        SELECT COUNT(DISTINCT input_file_name) AS file_count
        FROM {catalog}.bronze.{table}
        WHERE country_code = '{country}'
          AND bronze_inserted_at >= '{run_start_str}'
    """)
    num_files = distinct_files_df.collect()[0]["file_count"]
else:
    num_files = 0

print(f"[AutoLoader] country={country} table={table}")
print(f"[AutoLoader] Files processed this run : {num_files}")
print(f"[AutoLoader] Rows ingested this run   : {total_rows}")
if total_rows == 0:
    print("[AutoLoader] No new files detected — checkpoint is up to date. Nothing written to bronze.")
else:
    print(f"[AutoLoader] {total_rows} rows from {num_files} file(s) written to {catalog}.bronze.{table}")
