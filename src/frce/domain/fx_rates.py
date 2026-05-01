from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class FxRate:
    rate_id: str
    base_currency: str
    target_currency: str
    rate: float
    rate_date: date
    source: str = "ECB"
