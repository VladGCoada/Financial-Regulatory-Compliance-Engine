from frce.compliance.aml_rule_engine import AmlRuleEngine


def test_high_value_flagged(spark):
    data = [("p-001", 150_000.0, "EUR", "NL")]
    df = spark.createDataFrame(data, ["payment_id", "amount", "currency", "country_code"])
    result = AmlRuleEngine().apply(df).collect()[0]
    assert result["is_flagged"] is True
    assert "HIGH_VALUE_SINGLE_TXN" in result["aml_flags"]


def test_normal_transaction_not_flagged(spark):
    data = [("p-002", 500.0, "EUR", "NL")]
    df = spark.createDataFrame(data, ["payment_id", "amount", "currency", "country_code"])
    result = AmlRuleEngine().apply(df).collect()[0]
    assert result["is_flagged"] is False
    assert result["max_risk_score"] == 0.0


def test_sanctioned_country_high_risk(spark):
    data = [("p-003", 5_000.0, "USD", "IR")]
    df = spark.createDataFrame(data, ["payment_id", "amount", "currency", "country_code"])
    result = AmlRuleEngine().apply(df).collect()[0]
    assert result["max_risk_score"] == 0.9