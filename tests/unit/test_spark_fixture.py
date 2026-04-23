from pyspark.sql import SparkSession

def test_spark_session_available(spark):
    assert spark is not None
    assert isinstance(spark, SparkSession)