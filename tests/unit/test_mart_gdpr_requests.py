from frce.gold.mart_gdpr_requests import MartGdprRequestsTask
from frce.config import FrceConfig


def test_build_joins_requests_and_audit(spark):
    config = FrceConfig()
    task = MartGdprRequestsTask(config=config)
    task._spark = spark

    requests = spark.createDataFrame(
        [("req-001", "DEBTOR_IBAN", "NL91ABNA0417164300",
          "2024-01-10T08:00:00", "COMPLETED")],
        ["request_id", "entity_type", "entity_id", "requested_at", "status"]
    )
    audit = spark.createDataFrame(
        [("aud-001", "req-001", "2024-01-10T09:00:00", 5, 5, 2, "gdpr_erasure_pipeline")],
        ["audit_id", "request_id", "completed_at",
         "bronze_rows_deleted", "silver_rows_deleted",
         "gold_rows_deleted", "operator"]
    )
    result = task.build(requests, audit)
    row = result.collect()[0]
    assert row["request_id"] == "req-001"
    assert row["bronze_rows_deleted"] == 5
    assert row["operator"] == "gdpr_erasure_pipeline"