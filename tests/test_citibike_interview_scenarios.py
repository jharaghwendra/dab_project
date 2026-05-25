"""Interview-ready PySpark unit tests for the Citi Bike project.

These tests use tiny input datasets so expected outputs are easy to understand.

How to read this file quickly (interview revision cheat mode):
1) Read only comments that start with "Baby idea" -> this is the business rule.
2) Read "Input looks like" -> this is the mini test dataset.
3) Read "Expected output looks like" -> this is what you assert.
4) Final assert line is the proof that rule works.
"""

import datetime

from pyspark.sql.functions import avg, count, create_map, lit, max, min, round

from src.citibike.citibike_utils import get_trip_duration_mins
from src.utils.datetime_utils import timestamp_to_date_col


def test_01_trip_duration_basic_minutes(spark):
    # Baby idea: 10:00 -> 10:10 is 10 minutes, 10:00 -> 10:30 is 30 minutes.
    # Input looks like: [(started_at, ended_at), ...]
    # Expected output looks like: [(trip_duration_mins,), ...] = [(10.0,), (30.0,)]
    data = [
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 10, 0)),
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 30, 0)),
    ]
    df = spark.createDataFrame(data, "started_at timestamp, ended_at timestamp")

    out = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")
    # Collect the single column we care about; orderBy makes the result order predictable.
    actual = [row["trip_duration_mins"] for row in out.orderBy("trip_duration_mins").collect()]
    expected = [10.0, 30.0]
    assert actual == expected


def test_02_trip_duration_zero_minutes(spark):
    # Baby idea: same start and end time means 0 minutes.
    # Input looks like: [10:00 -> 10:00]
    # Expected output looks like: [0.0]
    data = [(datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 0, 0))]
    df = spark.createDataFrame(data, "started_at timestamp, ended_at timestamp")

    out = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")
    actual = [row["trip_duration_mins"] for row in out.collect()]
    expected = [0.0]
    assert actual == expected


def test_03_trip_duration_negative_if_end_before_start(spark):
    # Baby idea: bad data can happen; if end is before start, duration is negative.
    # Input looks like: [10:10 -> 10:00] (bad event order)
    # Expected output looks like: [-10.0]
    data = [(datetime.datetime(2025, 4, 10, 10, 10, 0), datetime.datetime(2025, 4, 10, 10, 0, 0))]
    df = spark.createDataFrame(data, "started_at timestamp, ended_at timestamp")

    out = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")
    actual = [row["trip_duration_mins"] for row in out.collect()]
    expected = [-10.0]
    assert actual == expected


def test_04_timestamp_to_date_extracts_calendar_day(spark):
    # Baby idea: keep only the date part from timestamp.
    # Input looks like: [2025-04-10 22:45:00, 2025-04-11 00:05:00]
    # Expected output looks like: [2025-04-10, 2025-04-11]
    data = [
        (datetime.datetime(2025, 4, 10, 22, 45, 0),),
        (datetime.datetime(2025, 4, 11, 0, 5, 0),),
    ]
    df = spark.createDataFrame(data, "started_at timestamp")

    out = timestamp_to_date_col(spark, df, "started_at", "trip_start_date")
    actual = [row["trip_start_date"] for row in out.orderBy("trip_start_date").collect()]
    expected = [datetime.date(2025, 4, 10), datetime.date(2025, 4, 11)]
    assert actual == expected


def test_05_silver_projection_matches_contract_columns(spark):
    # Baby idea: silver table should contain only the expected clean columns.
    # Input looks like: one raw ride row with extra columns.
    # Expected output looks like: only silver contract columns in fixed order.
    raw = [
        (
            "ride_1",
            datetime.datetime(2025, 4, 10, 10, 0, 0),
            datetime.datetime(2025, 4, 10, 10, 10, 0),
            "Station A",
            "Station B",
            "extra_field_we_do_not_need",
        )
    ]
    df = spark.createDataFrame(
        raw,
        "ride_id string, started_at timestamp, ended_at timestamp, "
        "start_station_name string, end_station_name string, extra_col string",
    )

    df = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")
    df = timestamp_to_date_col(spark, df, "started_at", "trip_start_date")
    df = df.withColumn(
        "metadata",
        create_map(
            lit("pipeline_id"),
            lit("p1"),
            lit("run_id"),
            lit("r1"),
            lit("task_id"),
            lit("t1"),
            lit("processed_date"),
            lit("2026-05-25T10:00:00"),
        ),
    )

    silver = df.select(
        "ride_id",
        "trip_start_date",
        "started_at",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "trip_duration_mins",
        "metadata",
    )

    assert silver.columns == [
        "ride_id",
        "trip_start_date",
        "started_at",
        "ended_at",
        "start_station_name",
        "end_station_name",
        "trip_duration_mins",
        "metadata",
    ]


def test_06_silver_metadata_contains_pipeline_fields(spark):
    # Baby idea: every row carries run metadata like an ID card.
    # Input looks like: one ride row + metadata map fields.
    # Expected output looks like: metadata has pipeline_id, run_id, task_id, processed_date.
    data = [
        (
            "ride_1",
            datetime.datetime(2025, 4, 10, 10, 0, 0),
            datetime.datetime(2025, 4, 10, 10, 10, 0),
        )
    ]
    df = spark.createDataFrame(data, "ride_id string, started_at timestamp, ended_at timestamp")

    out = df.withColumn(
        "metadata",
        create_map(
            lit("pipeline_id"),
            lit("pipe_123"),
            lit("run_id"),
            lit("run_456"),
            lit("task_id"),
            lit("task_789"),
            lit("processed_date"),
            lit("2026-05-25T10:00:00"),
        ),
    )

    row = out.select("metadata").collect()[0]["metadata"]
    assert row["pipeline_id"] == "pipe_123"
    assert row["run_id"] == "run_456"
    assert row["task_id"] == "task_789"
    assert row["processed_date"] == "2026-05-25T10:00:00"


def test_07_gold_daily_summary_single_day(spark):
    # Baby idea: one day with 3 trips -> max/min/avg/count should be easy math.
    # Input looks like: trip_duration_mins = [10.0, 20.0, 30.0] for one date.
    # Expected output looks like: max=30.0, min=10.0, avg=20.0, total_trips=3.
    data = [
        ("ride_1", datetime.date(2025, 4, 10), 10.0),
        ("ride_2", datetime.date(2025, 4, 10), 20.0),
        ("ride_3", datetime.date(2025, 4, 10), 30.0),
    ]
    df = spark.createDataFrame(data, "ride_id string, trip_start_date date, trip_duration_mins double")

    out = df.groupBy("trip_start_date").agg(
        round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
        round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
        round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips"),
    )

    actual = [
        (
            row["trip_start_date"],
            row["max_trip_duration_mins"],
            row["min_trip_duration_mins"],
            row["avg_trip_duration_mins"],
            row["total_trips"],
        )
        for row in out.orderBy("trip_start_date").collect()
    ]
    expected = [(datetime.date(2025, 4, 10), 30.0, 10.0, 20.0, 3)]
    assert actual == expected


def test_08_gold_daily_summary_multiple_days(spark):
    # Baby idea: each date should be aggregated separately.
    # Input looks like: 2 rows for 2025-04-10 and 1 row for 2025-04-11.
    # Expected output looks like: two summary rows, one per date.
    data = [
        ("ride_1", datetime.date(2025, 4, 10), 10.0),
        ("ride_2", datetime.date(2025, 4, 10), 20.0),
        ("ride_3", datetime.date(2025, 4, 11), 30.0),
    ]
    df = spark.createDataFrame(data, "ride_id string, trip_start_date date, trip_duration_mins double")

    out = df.groupBy("trip_start_date").agg(
        round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
        round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
        round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips"),
    )

    actual = [
        (
            row["trip_start_date"],
            row["max_trip_duration_mins"],
            row["min_trip_duration_mins"],
            row["avg_trip_duration_mins"],
            row["total_trips"],
        )
        for row in out.orderBy("trip_start_date").collect()
    ]
    expected = [
        (datetime.date(2025, 4, 10), 20.0, 10.0, 15.0, 2),
        (datetime.date(2025, 4, 11), 30.0, 30.0, 30.0, 1),
    ]
    assert actual == expected


def test_09_gold_station_performance_by_day_and_station(spark):
    # Baby idea: group by date + start station, then compute avg duration and trip count.
    # Input looks like: two rides from Station A, one from Station B (same day).
    # Expected output looks like: Station A -> avg 15.0 count 2, Station B -> avg 30.0 count 1.
    data = [
        ("ride_1", datetime.date(2025, 4, 10), "Station A", 10.0),
        ("ride_2", datetime.date(2025, 4, 10), "Station A", 20.0),
        ("ride_3", datetime.date(2025, 4, 10), "Station B", 30.0),
    ]
    df = spark.createDataFrame(
        data,
        "ride_id string, trip_start_date date, start_station_name string, trip_duration_mins double",
    )

    out = df.groupBy("trip_start_date", "start_station_name").agg(
        round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips"),
    )

    actual = [
        (row["trip_start_date"], row["start_station_name"], row["avg_trip_duration_mins"], row["total_trips"])
        for row in out.orderBy("trip_start_date", "start_station_name").collect()
    ]
    expected = [
        (datetime.date(2025, 4, 10), "Station A", 15.0, 2),
        (datetime.date(2025, 4, 10), "Station B", 30.0, 1),
    ]
    assert actual == expected


def test_10_gold_rounding_to_two_decimals(spark):
    # Baby idea: dashboard values should be rounded to 2 decimal places.
    # Input looks like: [10.111, 10.129]
    # Expected output looks like: rounded average = 10.12
    data = [
        ("ride_1", datetime.date(2025, 4, 10), 10.111),
        ("ride_2", datetime.date(2025, 4, 10), 10.129),
    ]
    df = spark.createDataFrame(data, "ride_id string, trip_start_date date, trip_duration_mins double")

    out = df.groupBy("trip_start_date").agg(round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"))

    actual = [row["avg_trip_duration_mins"] for row in out.collect()]
    expected = [10.12]
    assert actual == expected
