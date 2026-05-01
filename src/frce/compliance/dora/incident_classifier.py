from __future__ import annotations

from frce.domain.incidents import DoraIncident, Severity


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
            run_id,
            task_name,
            Severity.MAJOR,
            affected_clients,
            transaction_value_eur,
            duration_minutes,
            is_cross_border,
            "; ".join(rationale_parts),
            True,
        )

    if affected_clients >= 10_000 or transaction_value_eur >= 500_000 or duration_minutes >= 30:
        return DoraIncident(
            run_id,
            task_name,
            Severity.SIGNIFICANT,
            affected_clients,
            transaction_value_eur,
            duration_minutes,
            is_cross_border,
            "Mid-tier threshold met",
            is_cross_border,
        )

    return DoraIncident(
        run_id,
        task_name,
        Severity.MINOR,
        affected_clients,
        transaction_value_eur,
        duration_minutes,
        is_cross_border,
        "Below significant threshold",
        False,
    )


class DoraIncidentClassifier:
    def classify(
        self,
        run_id: str,
        task_name: str,
        affected_clients: int = 0,
        transaction_value_eur: float = 0.0,
        duration_minutes: int = 0,
        is_cross_border: bool = False,
    ) -> DoraIncident:
        return classify_incident(
            run_id,
            task_name,
            affected_clients,
            transaction_value_eur,
            duration_minutes,
            is_cross_border,
        )


DORAIncidentClassifier = DoraIncidentClassifier
