from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    MAJOR = "MAJOR"
    SIGNIFICANT = "SIGNIFICANT"
    MINOR = "MINOR"


@dataclass
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


def classify_incident(
    run_id: str,
    task_name: str,
    affected_clients: int = 0,
    transaction_value_eur: float = 0.0,
    duration_minutes: int = 0,
    is_cross_border: bool = False,
) -> DoraIncident:
    rationale_parts: list[str] = []

    if affected_clients >= 100_000:
        rationale_parts.append(f"affected_clients={affected_clients} >= 100,000")
    if transaction_value_eur >= 5_000_000:
        rationale_parts.append(f"transaction_value={transaction_value_eur:.0f} >= 5,000,000 EUR")
    if duration_minutes >= 120:
        rationale_parts.append(f"duration={duration_minutes}min >= 120min")

    if rationale_parts:
        return DoraIncident(
            run_id=run_id,
            task_name=task_name,
            severity=Severity.MAJOR,
            affected_clients_estimate=affected_clients,
            transaction_value_eur=transaction_value_eur,
            duration_minutes=duration_minutes,
            is_cross_border=is_cross_border,
            classification_rationale="; ".join(rationale_parts),
            eba_report_required=True,
        )

    if affected_clients >= 10_000 or transaction_value_eur >= 500_000 or duration_minutes >= 30:
        return DoraIncident(
            run_id=run_id,
            task_name=task_name,
            severity=Severity.SIGNIFICANT,
            affected_clients_estimate=affected_clients,
            transaction_value_eur=transaction_value_eur,
            duration_minutes=duration_minutes,
            is_cross_border=is_cross_border,
            classification_rationale="Mid-tier threshold met",
            eba_report_required=is_cross_border,
        )

    return DoraIncident(
        run_id=run_id,
        task_name=task_name,
        severity=Severity.MINOR,
        affected_clients_estimate=affected_clients,
        transaction_value_eur=transaction_value_eur,
        duration_minutes=duration_minutes,
        is_cross_border=is_cross_border,
        classification_rationale="Below significant threshold",
        eba_report_required=False,
    )
