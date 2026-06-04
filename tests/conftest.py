import sys
import os
import pytest

# Run the tests from the root directory
sys.path.append(os.getcwd())


# Returning a Spark Session
@pytest.fixture()
def spark():
    is_local_spark = False
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession

            # Protect local test runs from stale Spark shell variables.
            os.environ.pop("SPARK_HOME", None)
            os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
            os.environ.pop("SPARK_REMOTE", None)
            os.environ["PYSPARK_PYTHON"] = sys.executable

            spark = (
                SparkSession.builder.master("local[2]")
                .appName("pytest-local-pyspark")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
            is_local_spark = True
        except Exception as exc:
            raise ImportError("Neither Databricks Session nor local Spark Session is available") from exc

    try:
        yield spark
    finally:
        if is_local_spark:
            spark.stop()
