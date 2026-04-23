from frce.quality.dq_validator import DQValidator
from frce.quality.dq_rules import PAYMENT_DQ_RULES


def test_good_records_pass(spark):
    data = [
        ("p-001", 100.0, "EUR", "NL91ABNA0417164300"),
        ("p-002", 250.5, "USD", "DE89370400440532013000"),
    ]
    df = spark.createDataFrame(data, ["payment_id", "amount", "currency", "debtor_iban"])

    rules = [r for r in PAYMENT_DQ_RULES if r["name"] in (
        "payment_id_not_null", "amount_positive", "currency_valid", "debtor_iban_not_null"
    )]
    validator = DQValidator(rules)
    result = validator.validate(df)

    assert result.total_good == 2
    assert result.total_quarantine == 0


def test_bad_records_quarantined(spark):
    data = [
        (None, 100.0, "EUR", "NL91ABNA0417164300"),     # null payment_id
        ("p-002", -50.0, "EUR", "DE89370400440532013000"),  # negative amount
        ("p-003", 75.0, "XYZ", "NL91ABNA0417164300"),   # bad currency
    ]
    df = spark.createDataFrame(data, ["payment_id", "amount", "currency", "debtor_iban"])

    rules = [r for r in PAYMENT_DQ_RULES if r["name"] in (
        "payment_id_not_null", "amount_positive", "currency_valid"
    )]
    validator = DQValidator(rules)
    result = validator.validate(df)

    assert result.total_quarantine == 3
    assert result.total_good == 0
    assert "_dq_failure_reason" not in result.good.columns
    assert "quarantine_at" in result.quarantine.columns