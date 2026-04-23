from frce.transformations.silver_fx import SilverFxTask
from frce.config import FrceConfig


def test_clean_removes_non_eur_base(spark):
    config = FrceConfig()
    task = SilverFxTask(config=config)
    task._spark = spark

    data = [
        ("r1", "EUR", "USD", 1.08, "2024-01-15", "2024-01-15T10:00:00", "ECB"),
        ("r2", "USD", "GBP", 0.79, "2024-01-15", "2024-01-15T10:00:00", "ECB"),
    ]
    df = spark.createDataFrame(
        data, ["rate_id", "base_currency", "target_currency",
               "rate", "rate_date", "ingested_at", "source"]
    )
    result = task.clean(df)
    assert result.count() == 1
    assert result.collect()[0]["base_currency"] == "EUR"


def test_clean_deduplicates_by_latest(spark):
    config = FrceConfig()
    task = SilverFxTask(config=config)
    task._spark = spark

    data = [
        ("r1", "EUR", "USD", 1.07, "2024-01-15", "2024-01-15T09:00:00", "ECB"),
        ("r2", "EUR", "USD", 1.08, "2024-01-15", "2024-01-15T11:00:00", "ECB"),
    ]
    df = spark.createDataFrame(
        data, ["rate_id", "base_currency", "target_currency",
               "rate", "rate_date", "ingested_at", "source"]
    )
    result = task.clean(df)
    assert result.count() == 1
    assert result.collect()[0]["rate"] == 1.08