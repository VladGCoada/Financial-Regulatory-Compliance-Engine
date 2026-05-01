from __future__ import annotations

REQUIRED_COUNTERPARTY_COLUMNS = {
    "counterparty_id",
    "legal_name",
    "country_code",
    "iban",
    "email",
    "risk_tier",
}


def validate_counterparty_columns(columns: list[str]) -> list[str]:
    return sorted(REQUIRED_COUNTERPARTY_COLUMNS - set(columns))
