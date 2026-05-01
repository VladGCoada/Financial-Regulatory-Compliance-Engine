from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SepaPayment:
    payment_id: str
    transaction_reference: str
    debtor_iban: str | None
    creditor_iban: str | None
    debtor_name: str | None
    creditor_name: str | None
    debtor_email: str | None
    creditor_email: str | None
    amount: float
    currency: str
    country_code: str
    booked_at: datetime
    source_system: str
