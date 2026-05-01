from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelRiskAssessment:
    model_name: str
    model_version: str
    inherent_risk: str
    explainability_level: str
    monitoring_required: bool
    approval_required: bool
    rationale: str


def assess_model_risk(
    model_name: str,
    model_version: str,
    uses_pii: bool,
    makes_adverse_decisions: bool,
    is_high_volume: bool,
) -> ModelRiskAssessment:
    score = 0
    reasons: list[str] = []
    if uses_pii:
        score += 2
        reasons.append("uses PII-derived features")
    if makes_adverse_decisions:
        score += 3
        reasons.append("may influence adverse decisions")
    if is_high_volume:
        score += 1
        reasons.append("high-volume automated processing")

    if score >= 5:
        risk = "HIGH"
        explainability = "ROW_LEVEL"
        approval = True
    elif score >= 3:
        risk = "MEDIUM"
        explainability = "BATCH_LEVEL"
        approval = True
    else:
        risk = "LOW"
        explainability = "SUMMARY_LEVEL"
        approval = False

    return ModelRiskAssessment(
        model_name=model_name,
        model_version=model_version,
        inherent_risk=risk,
        explainability_level=explainability,
        monitoring_required=True,
        approval_required=approval,
        rationale="; ".join(reasons) or "low impact analytical model",
    )


def requires_human_approval(assessment: ModelRiskAssessment) -> bool:
    return assessment.approval_required or assessment.inherent_risk == "HIGH"
