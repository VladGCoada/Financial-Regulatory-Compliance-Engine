from frce.common_schemas import SEPA_PAYMENT_SCHEMA


def test_sepa_payment_schema_contains_expected_fields() -> None:
    field_names = [field.name for field in SEPA_PAYMENT_SCHEMA.fields]

    assert "payment_id" in field_names
    assert "debtor_iban" in field_names
    assert "creditor_iban" in field_names
    assert "amount" in field_names
    assert "ingested_at" in field_names