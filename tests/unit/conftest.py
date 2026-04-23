import os
import pytest
from pyspark.sql import SparkSession

# Windows fix: PySpark worker needs 'python', not 'python3'
os.environ.setdefault("PYSPARK_PYTHON", "python")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("frce-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()