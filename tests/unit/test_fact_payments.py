from frce.gold.fact_payments import FactPaymentsTask, _sha256_udf
from frce.config import FrceConfig


def test_mask_pii_removes_raw_iban(spark):
    config = FrceConfig()
    task = FactPaymentsTask(config=config)
    task._spark = spark

    data = [("p-001", "NL91ABNA0417164300", "DE89370400440532013000",
             "Alice BV", "Bob GmbH", "alice@acme.nl", "bob@gmbh.de")]
    df = spark.createDataFrame(
        data, ["payment_id", "debtor_iban", "creditor_iban",
               "debtor_name", "creditor_name", "debtor_email", "creditor_email"]
    )
    result = task.mask_pii(df)
    cols = result.columns
    assert "debtor_iban" not in cols
    assert "creditor_iban" not in cols
    assert "debtor_iban_hash" in cols
    assert "creditor_iban_hash" in cols


def test_sha256_hash_is_not_reversible(spark):
    config = FrceConfig()
    task = FactPaymentsTask(config=config)
    task._spark = spark

    data = [("p-001", "NL91ABNA0417164300")]
    df = spark.createDataFrame(data, ["payment_id", "debtor_iban"])
    result = df.withColumn("hash", _sha256_udf("debtor_iban"))
    row = result.collect()[0]
    assert row["hash"] != "NL91ABNA0417164300"
    assert len(row["hash"]) == 64