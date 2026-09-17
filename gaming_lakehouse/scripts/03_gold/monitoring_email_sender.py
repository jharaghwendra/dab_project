import argparse
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql.functions import col


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("summary_table", "")
dbutils.widgets.text("recipient_email", "")
dbutils.widgets.text("lookback_minutes", "60")

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("--catalog")
parser.add_argument("--summary_table")
parser.add_argument("--recipient_email")
parser.add_argument("--lookback_minutes")
args, _ = parser.parse_known_args(sys.argv[1:])

catalog = (args.catalog or dbutils.widgets.get("catalog") or "main").strip()
summary_table = (
    args.summary_table or dbutils.widgets.get("summary_table") or f"{catalog}.gold.pipeline_monitoring_summary"
).strip()
recipient_email = (args.recipient_email or dbutils.widgets.get("recipient_email") or "").strip()
lookback_minutes = int(args.lookback_minutes or dbutils.widgets.get("lookback_minutes") or "60")

# This script is intentionally minimal for the portfolio: it reads the analyzer-produced summary
# rows from Delta and emits a plain operational email body. Wire the actual mail service here.
#
# Each for_each_task iteration writes its own row (see monitoring_summary_analyzer.py), so this
# task aggregates every row written in the current run's lookback window into one digest email,
# rather than reading a single "latest row".

if not spark.catalog.tableExists(summary_table):
    raise ValueError(f"Monitoring summary table not found: {summary_table}")

run_window_start = (datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)).isoformat()
summary_rows = (
    spark.table(summary_table)
    .filter(col("created_at") >= run_window_start)
    .orderBy(col("country_code"), col("table_name"))
    .collect()
)

if not summary_rows:
    raise ValueError(f"No monitoring summary rows found in {summary_table} for the last {lookback_minutes} minutes.")

digest_text = "\n\n".join(row["summary_text"] for row in summary_rows)

subject = f"iGaming Pipeline Monitoring Summary - {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
body = f"""Hello,

Here is the latest iGaming pipeline monitoring summary ({len(summary_rows)} table/country checks):

{digest_text}

Source table: {summary_table}
Recipient: {recipient_email or "not configured"}

Regards,
Databricks Monitoring Job
"""

print(f"[MonitoringEmail] Subject: {subject}")
print(f"[MonitoringEmail] Body:\n{body}")

# Example extension point:
# - send via SMTP, Microsoft Graph, SendGrid, or Databricks notification integration
