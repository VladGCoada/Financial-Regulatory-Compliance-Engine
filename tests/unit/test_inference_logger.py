from frce.intelligence.inference_logger import InferenceLogger, _schema_hash
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def test_schema_hash_is_deterministic(spark):
    schema = StructType([
        StructField("a", StringType(), True),
        StructField("b", DoubleType(), True),
    ])
    df = spark.createDataFrame([], schema)
    h1 = _schema_hash(df.schema)
    h2 = _schema_hash(df.schema)
    assert h1 == h2
    assert len(h1) == 16


def test_schema_hash_differs_for_different_schemas(spark):
    schema_a = StructType([StructField("a", StringType(), True)])
    schema_b = StructType([StructField("b", StringType(), True)])
    df_a = spark.createDataFrame([], schema_a)
    df_b = spark.createDataFrame([], schema_b)
    assert _schema_hash(df_a.schema) != _schema_hash(df_b.schema)


def test_inference_logger_required_fields_exist():
    required = {
        "inference_id", "model_name", "model_version", "mlflow_run_id",
        "training_data_version", "input_data_version", "scored_at",
        "input_row_count", "output_schema_hash", "explainability_sample"
    }
    import inspect
    src = inspect.getsource(InferenceLogger.log)
    for field in required:
        assert field in src, f"Missing EU AI Act field: {field}"