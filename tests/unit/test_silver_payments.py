from unittest.mock import MagicMock, patch
from frce.transformations.silver_payments import SilverPaymentsTask
from frce.config import FrceConfig


def test_cast_and_clean_normalises_currency(spark):
    config = FrceConfig()
    task = SilverPaymentsTask(config=config)
    task._spark = spark

    data = [("p-001", "100.0", "eur", "NL91ABNA0417164300", "nl")]
    df = spark.createDataFrame(data, ["payment_id", "amount", "currency", "debtor_iban", "country_code"])

    result = task.cast_and_clean(df)
    row = result.collect()[0]

    assert row["currency"] == "EUR"
    assert row["country_code"] == "NL"
    assert isinstance(row["amount"], float)


def test_silver_payments_task_instantiates():
    config = FrceConfig()
    task = SilverPaymentsTask(config=config)
    assert task.validator is not None