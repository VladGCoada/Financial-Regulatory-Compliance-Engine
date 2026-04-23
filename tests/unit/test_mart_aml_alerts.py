from frce.gold.mart_aml_alerts import MartAmlAlertsTask
from frce.config import FrceConfig


def test_build_filters_to_flagged_only(spark):
    config = FrceConfig()
    task = MartAmlAlertsTask(config=config)
    task._spark = spark

    data = [
        ("p-001", "TXN-001", 150_000.0, "EUR", "NL", "2024-01-15T10:00:00"),
        ("p-002", "TXN-002", 200.0, "EUR", "NL", "2024-01-15T10:00:00"),
    ]
    df = spark.createDataFrame(
        data, ["payment_id", "transaction_reference",
               "amount", "currency", "country_code", "booked_at"]
    )
    result = task.build(df)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["payment_id"] == "p-001"