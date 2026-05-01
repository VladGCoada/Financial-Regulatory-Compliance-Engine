from __future__ import annotations

PII_COLUMNS = {
    "debtor_iban",
    "creditor_iban",
    "debtor_name",
    "creditor_name",
    "debtor_email",
    "creditor_email",
    "iban",
    "email",
    "legal_name",
    "entity_id",
}


def contains_pii(column: str) -> bool:
    return column in PII_COLUMNS
