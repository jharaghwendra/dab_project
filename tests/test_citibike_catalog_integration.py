"""Integration tests that run against the real citibike_dev Databricks catalog.

Unlike the unit tests (test_citibike_scenarios.py) which use tiny
hardcoded data, these tests read actual catalog tables via Databricks Connect
and assert DATA QUALITY RULES instead of exact values.

Tables under test:
  citibike_dev.01_bronze.jc_citibike
  citibike_dev.02_silver.jc_citibike
  citibike_dev.03_gold.daily_ride_summary
  citibike_dev.03_gold.daily_station_performance

How to run (requires Databricks Connect venv):
  .venv_dbc\\Scripts\\Activate.ps1
  pytest tests/test_citibike_catalog_integration.py -v

These tests are SKIPPED automatically when Databricks Connect is not available
(e.g. when running with the local PySpark venv .venv_pyspark).
"""

import pytest

CATALOG = "citibike_dev"

# ---------------------------------------------------------------------------
# Shared fixture: skip the whole file if Databricks Connect is not installed
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dbc_spark():
    """Return a Databricks Connect SparkSession, or skip the test module."""
    try:
        from databricks.connect import DatabricksSession
        import os

        os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
        spark = DatabricksSession.builder.serverless().getOrCreate()
        yield spark
    except ImportError:
        pytest.skip("Databricks Connect not installed — skipping catalog integration tests")


# ---------------------------------------------------------------------------
# BRONZE layer tests  (citibike_dev.01_bronze.jc_citibike)
# ---------------------------------------------------------------------------


def test_01_bronze_table_has_rows(dbc_spark):
    #  Test: bronze table must have data loaded — empty means pipeline never ran.
    # Rule: row count > 0
    df = dbc_spark.read.table(f"{CATALOG}.01_bronze.jc_citibike")

    actual = df.count()
    assert actual > 0, f"Bronze table is empty — expected at least 1 row, got {actual}"


def test_02_bronze_ride_id_is_never_null(dbc_spark):
    #  Test: ride_id is the primary key — a null means bad ingestion.
    # Rule: zero rows where ride_id IS NULL
    df = dbc_spark.read.table(f"{CATALOG}.01_bronze.jc_citibike")

    actual = df.filter("ride_id IS NULL").count()
    expected = 0
    assert actual == expected, f"Found {actual} rows with NULL ride_id in bronze"


def test_03_bronze_started_at_before_ended_at_for_most_rows(dbc_spark):
    #  Test: for valid rides, start time must be before end time.
    # Rule: at least 99% of rows have started_at <= ended_at  (allow tiny % of bad data)
    df = dbc_spark.read.table(f"{CATALOG}.01_bronze.jc_citibike")

    total = df.count()
    bad = df.filter("started_at > ended_at").count()
    bad_pct = (bad / total) * 100

    assert bad_pct < 1.0, f"Too many rows where ended_at < started_at: {bad}/{total} = {bad_pct:.2f}%"


# ---------------------------------------------------------------------------
# SILVER layer tests  (citibike_dev.02_silver.jc_citibike)
# ---------------------------------------------------------------------------


def test_04_silver_has_contract_columns(dbc_spark):
    #  Test: silver schema must match the agreed contract — no column renames allowed.
    # Rule: exact column list matches
    df = dbc_spark.read.table(f"{CATALOG}.02_silver.jc_citibike")

    actual = df.columns
    expected = [
        "ride_id",
        "trip_start_date",
        "started_at",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "trip_duration_mins",
        "metadata",
    ]
    assert actual == expected, f"Silver column mismatch.\nActual:   {actual}\nExpected: {expected}"


def test_05_silver_trip_duration_mins_not_null(dbc_spark):
    #  Test: trip_duration_mins is a computed column — null means the function failed.
    # Rule: zero null trip_duration_mins
    df = dbc_spark.read.table(f"{CATALOG}.02_silver.jc_citibike")

    actual = df.filter("trip_duration_mins IS NULL").count()
    expected = 0
    assert actual == expected, f"Found {actual} rows with NULL trip_duration_mins in silver"


def test_06_silver_trip_start_date_not_null(dbc_spark):
    #  Test: trip_start_date is derived from started_at — null means timestamp_to_date failed.
    # Rule: zero null trip_start_date values
    df = dbc_spark.read.table(f"{CATALOG}.02_silver.jc_citibike")

    actual = df.filter("trip_start_date IS NULL").count()
    expected = 0
    assert actual == expected, f"Found {actual} rows with NULL trip_start_date in silver"


def test_07_silver_metadata_has_all_required_keys(dbc_spark):
    #  Test: every row must carry pipeline lineage — missing keys mean broken metadata logic.
    # Rule: first row metadata map contains all 4 expected keys
    df = dbc_spark.read.table(f"{CATALOG}.02_silver.jc_citibike")

    row = df.select("metadata").limit(1).collect()[0]["metadata"]

    expected_keys = ["pipeline_id", "run_id", "task_id", "processed_timestamp"]
    for key in expected_keys:
        assert key in row, f"Metadata key '{key}' missing from silver row. Found keys: {list(row.keys())}"


# ---------------------------------------------------------------------------
# GOLD layer tests  (citibike_dev.03_gold.daily_ride_summary)
#                   (citibike_dev.03_gold.daily_station_performance)
# ---------------------------------------------------------------------------


def test_08_gold_daily_summary_one_row_per_date(dbc_spark):
    #  Test: daily_ride_summary groups by date — duplicate dates mean groupBy is broken.
    # Rule: total rows == distinct trip_start_date count
    df = dbc_spark.read.table(f"{CATALOG}.03_gold.daily_ride_summary")

    total_rows = df.count()
    distinct_dates = df.select("trip_start_date").distinct().count()

    assert total_rows == distinct_dates, (
        f"Duplicate dates in gold daily_ride_summary: {total_rows} rows but only {distinct_dates} distinct dates"
    )


def test_09_gold_daily_summary_max_gte_avg_gte_min(dbc_spark):
    #  Test: basic math — max can never be less than avg, avg can never be less than min.
    # Rule: zero rows where max < avg or avg < min
    from pyspark.sql.functions import col

    df = dbc_spark.read.table(f"{CATALOG}.03_gold.daily_ride_summary")

    bad_rows = df.filter(
        (col("max_trip_duration_in_mins") < col("avg_trip_duration_in_mins"))
        | (col("avg_trip_duration_in_mins") < col("min_trip_duration_in_mins"))
    ).count()

    expected = 0
    assert bad_rows == expected, f"Found {bad_rows} rows where max < avg or avg < min in gold daily_ride_summary"


def test_10_gold_station_performance_one_row_per_date_and_station(dbc_spark):
    #  Test: groupBy(date, station) must produce unique combos — duplicates mean groupBy failed.
    # Rule: total rows == distinct (trip_start_date, start_station_name) combos
    df = dbc_spark.read.table(f"{CATALOG}.03_gold.daily_station_performance")

    total_rows = df.count()
    distinct_combos = df.select("trip_start_date", "start_station_name").distinct().count()

    assert total_rows == distinct_combos, (
        f"Duplicate (date, station) combos in gold daily_station_performance: "
        f"{total_rows} rows but only {distinct_combos} distinct combos"
    )
