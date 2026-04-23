from frce.common_schemas import COUNTERPARTY_SCHEMA, FX_RATE_SCHEMA

def test_counterparty_schema_fields():
    names = [f.name for f in COUNTERPARTY_SCHEMA.fields]
    assert "counterparty_id" in names
    assert "iban" in names
    assert "risk_tier" in names

def test_fx_rate_schema_fields():
    names = [f.name for f in FX_RATE_SCHEMA.fields]
    assert "base_currency" in names
    assert "rate" in names
    
    