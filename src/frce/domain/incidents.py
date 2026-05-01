from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    MAJOR = "MAJOR"
    SIGNIFICANT = "SIGNIFICANT"
    MINOR = "MINOR"


@dataclass(frozen=True)
class DoraIncident:
    run_id: str
    task_name: str
    severity: Severity
    affected_clients_estimate: int
    transaction_value_eur: float
    duration_minutes: int
    is_cross_border: bool
    classification_rationale: str
    eba_report_required: bool
