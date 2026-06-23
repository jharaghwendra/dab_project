import dlt
from pyspark.sql.functions import avg, count, round


@dlt.table(comment="Gold layer: daily ride performance metrics per station, including average duration and total trips")
def gold_daily_station_performance():
    return (
        dlt.read("silver_jc_citibike")
        .groupBy("trip_start_date", "start_station_name")
        .agg(
            round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
            count("ride_id").alias("total_trips"),
        )
    )
