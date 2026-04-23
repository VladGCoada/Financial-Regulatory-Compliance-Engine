"""
Data quality rules for SEPA payment validation.
Each rule is a dict: {name, condition_sql, severity, quarantine_reason}
"""
from __future__ import annotations

PAYMENT_DQ_RULES = [
    {
        "name": "payment_id_not_null",
        "condition_sql": "payment_id IS NOT NULL",
        "severity": "CRITICAL",
        "quarantine_reason": "Missing payment_id",
    },
    {
        "name": "amount_positive",
        "condition_sql": "amount > 0",
        "severity": "CRITICAL",
        "quarantine_reason": "Non-positive payment amount",
    },
    {
        "name": "currency_valid",
        "condition_sql": "currency IN ('EUR','USD','GBP','CHF','SEK','DKK','PLN','CZK')",
        "severity": "HIGH",
        "quarantine_reason": "Unrecognised currency code",
    },
    {
        "name": "debtor_iban_not_null",
        "condition_sql": "debtor_iban IS NOT NULL",
        "severity": "HIGH",
        "quarantine_reason": "Missing debtor IBAN — PII gap",
    },
    {
        "name": "booked_at_not_future",
        "condition_sql": "booked_at <= current_timestamp()",
        "severity": "MEDIUM",
        "quarantine_reason": "booked_at timestamp is in the future",
    },
]