from __future__ import annotations

REQUIRED_PAYMENT_COLUMNS = {
    "payment_id",
    "transaction_reference",
    "debtor_iban",
    "creditor_iban",
    "amount",
    "currency",
    "country_code",
    "booked_at",
}


def validate_payment_columns(columns: list[str]) -> list[str]:
    return sorted(REQUIRED_PAYMENT_COLUMNS - set(columns))
