from frce.gold.mart_dora_incidents import MartDoraIncidentsTask
from frce.config import FrceConfig


def test_classify_rows_produces_severity_field(spark):
    config = FrceConfig()
    task = MartDoraIncidentsTask(config=config)
    task._spark = spark

    data = [("run-001", "payments_task", "FAILED", None,
             "2024-01-15T10:00:00", "Something went wrong", "{}")]
    df = spark.createDataFrame(
        data, ["run_id", "task_name", "status", "started_at",
               "completed_at", "error_message", "metadata"]
    )
    incidents = task.classify_rows(df)
    assert len(incidents) == 1
    assert "severity" in incidents[0]
    assert incidents[0]["severity"] in ("MAJOR", "SIGNIFICANT", "MINOR")