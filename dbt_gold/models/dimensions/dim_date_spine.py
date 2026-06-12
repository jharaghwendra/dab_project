# =============================================================================
# dim_date_spine — dbt Python model (Phase 1: Python model mechanics)
# =============================================================================
# Equivalent to dim_date_time.sql — same output, written in Python.
#
# WHY Python here :
#   SQL version uses Spark-specific: sequence(), explode(), make_timestamp(),
#   from_utc_timestamp() — all Databricks/Spark SQL dialect.
#   Python version uses pandas.date_range() — portable, readable, testable locally.
#
# dbt Python model mechanics demonstrated:
#   1. def model(dbt, session) — entry point, always this signature on Databricks
#   2. dbt.config()            — set materialization inline (like the config() macro in SQL)
#   3. No dbt.ref() here       — date spine has no upstream dbt model dependency
#   4. session.createDataFrame()— session is the SparkSession, passed in by dbt
#   5. Explicit Spark schema   — avoids pandas type inference surprises
#   6. return final_df         — must return exactly one DataFrame
#
# Output: igaming_dev.gold.dim_date_spine
# Primary key: (sk_date_time, timezone)
# Grain: one row per UTC hour × timezone (UTC + Europe/Vienna)
# Range: 2025-01-01 → 2026-12-31 (extendable by changing START/END below)
# =============================================================================

import pandas as pd
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DateType,
    ByteType,
    StringType,
)


def model(dbt, session):
    # -------------------------------------------------------------------------
    # Config
    # NOTE: materialized=table, file_format=delta, schema=gold are already set
    # in dbt_project.yml under dimensions: block — repeating here for clarity
    # so the file is self-documenting when read standalone.
    # -------------------------------------------------------------------------
    dbt.config(
        materialized="table",
        submission_method="serverless_cluster",  # correct value per dbt-databricks docs (NOT 'serverless')
        # 4 valid values for submission_method (dbt-databricks v1.9+):
        #   'all_purpose_cluster'  — uses existing running cluster (default, needs cluster_id or http_path)
        #   'job_cluster'          — spins up a new cluster per run (needs job_cluster_config)
        #   'serverless_cluster'   — uses Databricks Serverless Compute (no cluster config needed)
        #   'workflow_job'         — creates/reuses a Databricks Workflow
        # SQL models always run on SQL Warehouse regardless of this setting
    )

    # -------------------------------------------------------------------------
    # Constants — change these to extend the date range
    # -------------------------------------------------------------------------
    START = "2025-01-01"
    END = "2026-12-31 23:00:00"
    TIMEZONES = ["UTC", "Europe/Vienna"]

    # -------------------------------------------------------------------------
    # Step 1: generate all UTC hours using pandas date_range
    # pandas is the right tool here — small static dataset (~35K rows),
    # tz_convert() handles CET/CEST daylight saving transitions correctly
    # -------------------------------------------------------------------------
    utc_hours = pd.date_range(
        start=START,
        end=END,
        freq="h",  # one entry per hour
        tz="UTC",
    )

    # -------------------------------------------------------------------------
    # Step 2: build rows — one per (utc_hour × timezone)
    # This replicates the SQL model's CROSS JOIN timezones + from_utc_timestamp()
    # -------------------------------------------------------------------------
    rows = []
    for tz_name in TIMEZONES:
        # tz_convert handles DST automatically — e.g. 2025-03-30 Europe/Vienna
        # shifts from UTC+1 (CET) to UTC+2 (CEST) at 02:00 local time
        local_hours = utc_hours.tz_convert(tz_name)

        for ts_local in local_hours:
            # sk_date_time: YYYYMMDDHH in LOCAL time
            # Matches the format used in fact_gametransaction_kpi and fact_payments
            # so BI tools can join dim_date_spine to any fact on sk_date_time
            sk = int(ts_local.year * 1_000_000 + ts_local.month * 10_000 + ts_local.day * 100 + ts_local.hour)
            rows.append(
                (
                    sk,
                    ts_local.date(),  # Python datetime.date → Spark DateType
                    ts_local.hour,  # 0-23 → ByteType (fits -128 to 127)
                    ts_local.day,  # 1-31 → ByteType
                    ts_local.month,  # 1-12 → ByteType
                    ts_local.year,  # 2025-2026 → IntegerType
                    ts_local.quarter,  # 1-4 → ByteType  (pandas Timestamp property)
                    tz_name,
                )
            )

    # -------------------------------------------------------------------------
    # Step 3: define explicit Spark schema
    # WHY explicit schema (not inferred from pandas):
    #   pandas int64 → Spark LongType by default
    #   We want IntegerType for sk_date_time/year, ByteType for hour/day/month/quarter
    #   This matches the CAST expressions in the SQL equivalent exactly
    # -------------------------------------------------------------------------
    schema = StructType(
        [
            StructField("sk_date_time", IntegerType(), False),
            StructField("date", DateType(), False),
            StructField("hour", ByteType(), False),
            StructField("day", ByteType(), False),
            StructField("month", ByteType(), False),
            StructField("year", IntegerType(), False),
            StructField("quarter", ByteType(), False),
            StructField("timezone", StringType(), False),
        ]
    )

    # -------------------------------------------------------------------------
    # Step 4: create Spark DataFrame from Python list of tuples + explicit schema
    # session = SparkSession — passed in by dbt, equivalent to 'spark' in notebooks
    # -------------------------------------------------------------------------
    return session.createDataFrame(rows, schema=schema)
