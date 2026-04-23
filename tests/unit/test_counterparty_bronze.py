from datetime import datetime, timezone

from frce.ingestion.counterparty_bronze import CounterpartyBronzeTask
from frce.config import FrceConfig


def test_counterparty_bronze_task_instantiates():
    config = FrceConfig()
    task = CounterpartyBronzeTask(config=config, source_path="/tmp/fake.csv")
    assert task.source_path == "/tmp/fake.csv"
    assert task.config.catalog == "frce_dev"


def test_add_metadata_adds_ingested_at(spark):
    from frce.common_schemas import COUNTERPARTY_SCHEMA

    config = FrceConfig()
    task = CounterpartyBronzeTask(config=config, source_path="/tmp/fake.csv")
    task._spark = spark

    data = [
        (
            "cp-001",
            "ACME BV",
            "NL",
            "NL91ABNA0417164300",
            "acme@acme.nl",
            "LOW",
            datetime(2023, 1, 1, tzinfo=timezone.utc),
            None,
            None,
        )
    ]

    df = spark.createDataFrame(data, schema=COUNTERPARTY_SCHEMA)
    result = task.add_metadata(df)

    assert "ingested_at" in result.columns
    assert "source_file" in result.columns