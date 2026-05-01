from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AmlRule:
    flag: str
    condition_sql: str
    risk_score: float
    description: str
    regulatory_basis: str = "DORA Article 9"


DEFAULT_AML_RULES: list[AmlRule] = [
    AmlRule(
        flag="HIGH_VALUE_SINGLE_TXN",
        condition_sql="amount > 100000",
        risk_score=0.6,
        description="Single SEPA transaction exceeds EUR 100,000",
        regulatory_basis="DORA Article 9 / EU AML Directive",
    ),
    AmlRule(
        flag="HIGH_RISK_JURISDICTION",
        condition_sql="country_code IN ('RU','BY','IR','KP','SY') AND amount > 1000",
        risk_score=0.9,
        description="Transaction involving a high-risk jurisdiction",
        regulatory_basis="FATF Recommendation 19 / EU AML Directive Article 18",
    ),
    AmlRule(
        flag="ROUND_AMOUNT",
        condition_sql="amount % 10000 = 0 AND amount >= 10000",
        risk_score=0.3,
        description="Suspiciously round large amount",
        regulatory_basis="FATF Recommendation 20",
    ),
]


def get_rule_by_flag(flag: str) -> AmlRule | None:
    return next((rule for rule in DEFAULT_AML_RULES if rule.flag == flag), None)


def get_rules_above_risk_score(threshold: float) -> list[AmlRule]:
    return [rule for rule in DEFAULT_AML_RULES if rule.risk_score >= threshold]
