import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp, lit


# Rather than resetting the production Auto Loader checkpoint, this task uses an
# isolated checkpoint and a bounded file-modification window. The live 10-minute
# ingestion job can continue running while this job appends to the same Bronze table.
dbutils.widgets.text("country_code", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("backfill_from_date", "")
dbutils.widgets.text("backfill_to_date", "")

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--country_code")
parser.add_argument("--table_name")
parser.add_argument("--catalog")
parser.add_argument("--backfill_from_date")
parser.add_argument("--backfill_to_date")
args, _ = parser.parse_known_args(sys.argv[1:])

country = (args.country_code or dbutils.widgets.get("country_code") or "").strip()
table = (args.table_name or dbutils.widgets.get("table_name") or "").strip()
catalog = (args.catalog or dbutils.widgets.get("catalog") or "main").strip()
from_date = (args.backfill_from_date or dbutils.widgets.get("backfill_from_date") or "").strip()
to_date = (args.backfill_to_date or dbutils.widgets.get("backfill_to_date") or "").strip()

if not country:
    raise ValueError("Missing critical task parameter: 'country_code'.")
if not table:
    raise ValueError("Missing critical task parameter: 'table_name'.")
if not from_date or not to_date:
    raise ValueError("Both 'backfill_from_date' and 'backfill_to_date' are required.")

try:
    parsed_from = datetime.strptime(from_date, "%Y-%m-%d")
    parsed_to = datetime.strptime(to_date, "%Y-%m-%d")
except ValueError as exc:
    raise ValueError("Backfill dates must use YYYY-MM-DD format.") from exc

if parsed_from >= parsed_to:
    raise ValueError("'backfill_to_date' must be after 'backfill_from_date'.")

# Auto Loader's modifiedBefore boundary is exclusive. The requested window is
# therefore [from_date 00:00:00Z, to_date 00:00:00Z).
from_timestamp = f"{from_date}T00:00:00.000Z"
to_timestamp = f"{to_date}T00:00:00.000Z"
window_key = f"{from_date}_{to_date}"

source_path = f"/Volumes/{catalog}/bronze/raw_landing/{country}/{table}/"
checkpoint_path = f"/Volumes/{catalog}/bronze/raw_landing/_bronze_checkpoints_backfill/{country}/{table}/{window_key}/"
schema_evolution_path = f"/Volumes/{catalog}/bronze/raw_landing/_schemas_backfill/{country}/{table}/{window_key}/"

# Fail cleanly before schema inference when the requested source folder is empty.
try:
    source_files = dbutils.fs.ls(source_path)
    parquet_files = [file_info for file_info in source_files if file_info.name.endswith(".parquet")]
except Exception:
    parquet_files = []

if not parquet_files:
    print(f"[AutoLoader Backfill] country={country} table={table}")
    print(f"[AutoLoader Backfill] Source folder is empty or has no parquet files: {source_path}")
    raise SystemExit(0)

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_evolution_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.modifiedAfter", from_timestamp)
    .option("cloudFiles.modifiedBefore", to_timestamp)
    .load(source_path)
)

enriched_stream = (
    raw_stream.withColumn("country_code", lit(country))
    .withColumn("input_file_name", col("_metadata.file_path"))
    .withColumn("bronze_inserted_at", current_timestamp())
)

query = (
    enriched_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.bronze.{table}")
)

run_start_time = datetime.now(timezone.utc)
query.awaitTermination()

total_rows = sum(progress.get("numInputRows", 0) for progress in query.recentProgress)
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

print(f"[AutoLoader Backfill] country={country} table={table}")
print(f"[AutoLoader Backfill] Window (UTC, end exclusive): {from_date} <= modified < {to_date}")
print(f"[AutoLoader Backfill] Files processed: {num_files}")
print(f"[AutoLoader Backfill] Rows ingested: {total_rows}")
if total_rows == 0:
    print("[AutoLoader Backfill] No files matched the requested window or this window was already processed.")
else:
    print(f"[AutoLoader Backfill] Appended {total_rows} rows to {catalog}.bronze.{table}")
