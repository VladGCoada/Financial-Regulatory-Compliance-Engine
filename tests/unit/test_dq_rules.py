from frce.quality.dq_rules import PAYMENT_DQ_RULES


def test_all_rules_have_required_keys():
    required = {"name", "condition_sql", "severity", "quarantine_reason"}
    for rule in PAYMENT_DQ_RULES:
        assert required.issubset(rule.keys()), f"Rule missing keys: {rule}"


def test_critical_rules_exist():
    names = [r["name"] for r in PAYMENT_DQ_RULES]
    assert "payment_id_not_null" in names
    assert "amount_positive" in names


def test_severity_values_are_valid():
    valid = {"CRITICAL", "HIGH", "MEDIUM", "LOW"}
    for rule in PAYMENT_DQ_RULES:
        assert rule["severity"] in valid