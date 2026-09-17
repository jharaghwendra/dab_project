import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

# STEPS — WHAT THIS SCRIPT DOES (runs once per table/country, called 27x by the
# for_each_task loop in igaming_pipeline_monitoring_job.yml):
#   1. Gets one "target" string per run, e.g. "de|ig_transaction" (country|table).
#   2. Splits it into country="de" and table_name="ig_transaction".
#   3. Builds the two table names to check: bronze.ig_transaction and silver.ig_transaction.
#   4. Works out a lookback window (default last 60 minutes) so it only checks recent activity.
#   5. CHECK 1 — Bronze freshness: counts rows that landed in bronze_table for this country in
#      the lookback window. 0 rows (or missing table) becomes a finding (a warning line).
#   6. CHECK 2 — Silver merge health: reads DESCRIBE HISTORY on silver_table for MERGE ops that
#      ran in the lookback window. A MERGE that inserted 0 rows AND updated 0 rows is suspicious
#      (it ran but did nothing) and is counted as an anomaly.
#   7. If nothing suspicious turned up, writes one friendly "no anomalies" finding instead of
#      leaving the report empty.
#   8. Joins all findings into one readable summary_text block.
#   9. Appends ONE row (below) to the shared Delta table {catalog}.gold.pipeline_monitoring_summary.
#      Since this runs 27x (one per table/country), that table gets 27 new rows per job run —
#      the email task later reads all of them together to build one combined digest.
# SAMPLE MULTI-ROW VIEW of pipeline_monitoring_summary after one job run (2 countries x
# 3 tables shown here out of the full 3x9 = 27 rows written per run):
#   created_at                        | country_code | table_name          | summary_text (truncated)                                                    | source
#   2026-09-01T09:05:12.345+00:00     | de           | ig_transaction      | No anomalies detected for ig_transaction/de in the last 60 minutes.        | rule_based_analyzer
#   2026-09-01T09:05:13.128+00:00     | de           | ig_player           | No new Bronze rows for ig_player/de in the last 60 minutes.                | rule_based_analyzer
#   2026-09-01T09:05:13.902+00:00     | de           | ig_payment          | 1 MERGE operation(s) on silver.ig_payment wrote 0 rows in the window.      | rule_based_analyzer
#   2026-09-01T09:05:14.611+00:00     | at           | ig_transaction      | No anomalies detected for ig_transaction/at in the last 60 minutes.        | rule_based_analyzer
#   2026-09-01T09:05:15.330+00:00     | at           | ig_player           | No anomalies detected for ig_player/at in the last 60 minutes.             | rule_based_analyzer
#   2026-09-01T09:05:16.045+00:00     | at           | ig_payment          | Silver table tma_dev.silver.ig_payment does not exist yet.                 | rule_based_analyzer

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("target", "de|ig_transaction")
dbutils.widgets.text("lookback_minutes", "60")
dbutils.widgets.text("summary_table", "")

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--catalog")
parser.add_argument("--target")
parser.add_argument("--lookback_minutes")
parser.add_argument("--summary_table")
args, _ = parser.parse_known_args(sys.argv[1:])

catalog = (args.catalog or dbutils.widgets.get("catalog") or "main").strip()
target = (args.target or dbutils.widgets.get("target") or "de|ig_transaction").strip()
lookback_minutes = int(args.lookback_minutes or dbutils.widgets.get("lookback_minutes") or "60")
summary_table = (
    args.summary_table or dbutils.widgets.get("summary_table") or f"{catalog}.gold.pipeline_monitoring_summary"
).strip()

# target comes from for_each_task's {{input}} — one "country|table" string per iteration.
country, table_name = target.split("|", 1)

# Rule-based pipeline health check — see igaming_pipeline_monitoring_job.yml header for why
# this isn't an LLM-based task. Each for_each_task iteration runs as its own task instance, so
# taskValues can't be aggregated by a single downstream task_key across iterations — this script
# writes its own row directly to Delta instead, and the email task reads all rows from this run.

bronze_table = f"{catalog}.bronze.{table_name}"
silver_table = f"{catalog}.silver.{table_name}"
lookback_start = (datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)).strftime("%Y-%m-%d %H:%M:%S")

findings = []

bronze_rows = 0
if spark.catalog.tableExists(bronze_table):
    bronze_rows = (
        spark.table(bronze_table)
        .filter(f"country_code = '{country}' AND bronze_inserted_at >= '{lookback_start}'")
        .count()
    )
else:
    findings.append(f"Bronze table {bronze_table} does not exist yet.")

if bronze_rows == 0 and not findings:
    findings.append(f"No new Bronze rows for {table_name}/{country} in the last {lookback_minutes} minutes.")

merge_anomaly_count = 0
if spark.catalog.tableExists(silver_table):
    merge_history = spark.sql(f"""
        SELECT operationMetrics
        FROM (DESCRIBE HISTORY {silver_table})
        WHERE operation = 'MERGE'
          AND timestamp >= '{lookback_start}'
    """).collect()
    for row in merge_history:
        metrics = row["operationMetrics"] or {}
        if (
            int(metrics.get("numTargetRowsUpdated", 0) or 0) == 0
            and int(metrics.get("numTargetRowsInserted", 0) or 0) == 0
        ):
            merge_anomaly_count += 1
    if merge_anomaly_count > 0:
        findings.append(
            f"{merge_anomaly_count} MERGE operation(s) on {silver_table} wrote 0 rows in the lookback window."
        )
else:
    findings.append(f"Silver table {silver_table} does not exist yet.")

if not findings:
    findings.append(f"No anomalies detected for {table_name}/{country} in the last {lookback_minutes} minutes.")

summary_text = (
    f"Pipeline health check for table={table_name} country={country} "
    f"(lookback={lookback_minutes}m):\n- " + "\n- ".join(findings)
)

print(f"[MonitoringAnalyzer] {summary_text}")

row = [
    {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "country_code": country,
        "table_name": table_name,
        "summary_text": summary_text,
        "summary_json": json.dumps({"summary_text": summary_text}),
        "source": "rule_based_analyzer",
    }
]
spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(summary_table)
print(f"[MonitoringAnalyzer] Wrote summary row to {summary_table}")
