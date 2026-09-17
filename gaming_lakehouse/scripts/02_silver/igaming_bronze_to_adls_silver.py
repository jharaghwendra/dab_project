import argparse
import sys
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from delta.tables import DeltaTable

from gaming_lakehouse.config.table_schemas import get_table_config

# 1. Capture dynamic execution parameters from the Workflow Task Row
dbutils.widgets.text("country_code", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("catalog", "main")

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--country_code")
parser.add_argument("--table_name")
parser.add_argument("--catalog")
args, _ = parser.parse_known_args(sys.argv[1:])

country = (args.country_code or dbutils.widgets.get("country_code") or "").strip()  # e.g., "de", "at", "dk"
table_name = (args.table_name or dbutils.widgets.get("table_name") or "").strip()  # e.g., "gametransaction", "wallet"
catalog = (args.catalog or dbutils.widgets.get("catalog") or "main").strip()

if not table_name:
    raise ValueError("Missing critical task parameter: 'table_name'.")

if not country:
    raise ValueError("Missing critical task parameter: 'country_code'.")

# 2. Load table config (schema + primary_key) from central registry
# To add a new table: define its schema in gaming_lakehouse/config/table_schemas.py
# and register it in TABLE_CONFIGS. No changes needed here.
_table_config = get_table_config(table_name)
primary_key = _table_config.primary_key
strict_schema = _table_config.schema
version_col = _table_config.version_col


# Plain Python counter (list for closure mutability — sparkContext.accumulator not supported on serverless)
# NOTE: total_inserts/total_updates are NOT tracked here — foreachBatch closures run in the remote
# PySpark Connect worker on serverless, so list mutations don't propagate back to the driver.
# Insert/update counts are collected from DESCRIBE HISTORY after awaitTermination() instead.
total_rows_processed = [0]


# 4. Define micro-batch execution logic function handler
def process_micro_batch(micro_batch_df, batch_id, current_country):
    # Dynamic schema enforcement generation loop expressions
    # Cast existing columns; null-fill schema columns absent from the batch (e.g., always-null
    # columns like availableBalancesAfter that parquet exporters omit when all-null).
    # This preserves a 1:1 column match with the registered Silver schema contract.
    existing_columns = set(micro_batch_df.columns)
    cast_expressions = [
        col(field.name).cast(field.dataType).alias(field.name)
        if field.name in existing_columns
        else lit(None).cast(field.dataType).alias(field.name)
        for field in strict_schema
    ]

    # Isolate memory partitions to deduplicate the 10-minute micro-batch data footprint
    window_spec = Window.partitionBy(primary_key).orderBy(col(version_col).desc())

    deduped_df = (
        micro_batch_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
    )

    # A missing source column is an expected schema gap. A non-null value that
    # becomes null after casting is a real data-quality failure to quarantine.
    cast_failure_checks = [
        col(field.name).isNotNull() & col(field.name).cast(field.dataType).isNull()
        for field in strict_schema
        if field.name in existing_columns
    ]
    has_cast_failure = (
        reduce(lambda left, right: left | right, cast_failure_checks) if cast_failure_checks else lit(False)
    )
    flagged_df = deduped_df.withColumn("_has_cast_failure", has_cast_failure).cache()

    rejected_df = flagged_df.filter(col("_has_cast_failure")).drop("_has_cast_failure")
    clean_source_df = flagged_df.filter(~col("_has_cast_failure")).drop("_has_cast_failure")
    clean_updates_df = (
        clean_source_df.select(*cast_expressions)  # Explicit type cast & rogue column drop guard
        .withColumn("country_code", lit(current_country))  # Re-attach after select; needed for multi-country merge key
        .withColumn("last_update", current_timestamp())  # Automated auditing stamp
        .cache()
    )

    try:
        # Target Table naming definitions managed by Unity Catalog
        target_table_name = f"{catalog}.silver.{table_name}"

        rejected_count = rejected_df.count()
        if rejected_count > 0:
            quarantine_schema = f"{catalog}.quarantine"
            quarantine_table = f"{quarantine_schema}.{table_name}"
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quarantine_schema}")
            (
                rejected_df.withColumn("_quarantine_reason", lit("schema_cast_failure"))
                .withColumn("_quarantine_batch_id", lit(batch_id))
                .withColumn("_quarantine_country", lit(current_country))
                .withColumn("_quarantined_at", current_timestamp())
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(quarantine_table)
            )
            print(
                f"[Silver batch_id={batch_id}] Quarantined {rejected_count} rows that failed schema cast "
                f"to {quarantine_table}."
            )

        batch_row_count = clean_updates_df.count()
        print(
            f"[Silver batch_id={batch_id}] country={current_country} table={table_name} | rows in batch (after dedup): {batch_row_count}"
        )
        if batch_row_count == 0:
            print(f"[Silver batch_id={batch_id}] Empty batch — nothing to upsert.")
            return
        total_rows_processed[0] += batch_row_count

        # Schema initialization checkpoint initialization on run day one
        if not spark.catalog.tableExists(target_table_name):
            clean_updates_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
            print(
                f"[Silver batch_id={batch_id}] Table did not exist — created {target_table_name} with {batch_row_count} rows."
            )
            return

        # Execute the High-Speed distributed Delta Lake Upsert Merge
        silver_delta_target = DeltaTable.forName(spark, target_table_name)
        update_mapping = {field: f"source.{field}" for field in clean_updates_df.columns}

        (
            silver_delta_target.alias("target")
            .merge(
                clean_updates_df.alias("source"),
                f"target.{primary_key} = source.{primary_key} AND target.country_code = source.country_code",
            )
            # Match condition equivalent to legacy SQL upsert pattern: update only when source version is newer
            .whenMatchedUpdate(condition=f"source.{version_col} > target.{version_col}", set=update_mapping)
            # Insertion tracking rules for completely brand new entities
            .whenNotMatchedInsert(values=update_mapping)
            .execute()
        )
        print(
            f"[Silver batch_id={batch_id}] Upsert complete — {batch_row_count} source rows merged into {target_table_name}."
        )
    finally:
        clean_updates_df.unpersist()
        flagged_df.unpersist()


# 5. Kick off Streaming processing and checkpoint logging tracking
streaming_bronze_df = (
    spark.readStream.format("delta").table(f"{catalog}.bronze.{table_name}").filter(col("country_code") == country)
)

checkpoint_path = f"/Volumes/{catalog}/bronze/raw_landing/_silver_checkpoints/{country}/{table_name}/"

# Capture silver row count before the stream runs — used for net-rows-written metric below.
# recentProgress / lastProgress are unreliable for availableNow=True + foreachBatch because
# data micro-batches complete and drop from the progress buffer before awaitTermination() returns.
# Pre/post table count is the ground truth equivalent to what we use in bronze.
_silver_table = f"{catalog}.silver.{table_name}"
pre_run_count = (
    spark.table(_silver_table).filter(col("country_code") == country).count()
    if spark.catalog.tableExists(_silver_table)
    else 0
)

# Record start time before stream kicks off — used to filter DESCRIBE HISTORY after run
run_start_time = datetime.now(timezone.utc)

query = (
    streaming_bronze_df.writeStream.foreachBatch(
        lambda micro_batch_df, batch_id: process_micro_batch(micro_batch_df, batch_id, country)
    )
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# 6. Post-run diagnostics — use table count delta as ground truth (stream metrics unreliable here)
post_run_count = spark.table(_silver_table).filter(col("country_code") == country).count()
net_rows_written = post_run_count - pre_run_count

# Collect insert/update counts from Delta history — sum all MERGE ops committed since run start.
# foreachBatch closures run in the remote PySpark Connect worker on serverless; Python list
# mutations inside the closure do not propagate back to the driver, so we read from history instead.
run_start_str = run_start_time.strftime("%Y-%m-%d %H:%M:%S")
merge_history = spark.sql(f"""
    SELECT operationMetrics
    FROM (DESCRIBE HISTORY {_silver_table})
    WHERE operation = 'MERGE'
      AND timestamp >= '{run_start_str}'
""").collect()

total_inserts_count = sum(int((r["operationMetrics"] or {}).get("numTargetRowsInserted", 0)) for r in merge_history)
total_updates_count = sum(
    int((r["operationMetrics"] or {}).get("numTargetRowsUpdated", 0))
    or int((r["operationMetrics"] or {}).get("numTargetRowsMatchedUpdated", 0))
    for r in merge_history
)

print(f"[Silver] country={country} table={table_name}")
print(f"[Silver] Silver rows before this run        : {pre_run_count}")
print(f"[Silver] Silver rows after this run         : {post_run_count}")
print(f"[Silver] Net new rows written to silver     : {net_rows_written}")
print(f"[Silver] Inserts this run                   : {total_inserts_count}")
print(f"[Silver] Updates this run                   : {total_updates_count}")
if net_rows_written == 0 and total_inserts_count == 0 and total_updates_count == 0:
    print("[Silver] No net change — either no new bronze data or all rows matched with equal/higher version.")
