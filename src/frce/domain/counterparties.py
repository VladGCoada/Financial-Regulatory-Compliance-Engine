from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Counterparty:
    counterparty_id: str
    legal_name: str
    country_code: str
    iban: str | None
    email: str | None
    risk_tier: str
    onboarded_at: datetime | None = None
