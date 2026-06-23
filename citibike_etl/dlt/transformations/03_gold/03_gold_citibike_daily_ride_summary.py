import dlt
from pyspark.sql.functions import avg, count, max, min, round


@dlt.table(comment="Gold layer: daily aggregates of ride durations and trip counts for Citi Bike data")
def gold_daily_ride_summary():
    return (
        dlt.read("silver_jc_citibike")
        .groupBy("trip_start_date")
        .agg(
            round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
            round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
            round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
            count("ride_id").alias("total_trips"),
        )
    )
