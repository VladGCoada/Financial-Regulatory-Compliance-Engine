from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ErasureRequest:
    request_id: str
    entity_type: str
    entity_id: str
    requested_at: datetime
    status: str = "PENDING"
