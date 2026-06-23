import dlt
from pyspark.sql.functions import col, to_date, unix_timestamp


@dlt.table(comment="Silver layer: cleaned and enriched Citi Bike data")
def silver_jc_citibike():
    return (
        dlt.read("bronze_jc_citibike")
        .withColumn(
            "trip_duration_mins",
            (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60,
        )
        .withColumn("trip_start_date", to_date(col("started_at")))
        .select(
            "ride_id",
            "trip_start_date",
            "started_at",
            "ended_at",
            "start_station_name",
            "end_station_name",
            "trip_duration_mins",
        )
    )
