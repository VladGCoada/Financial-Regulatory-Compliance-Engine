from __future__ import annotations

from dataclasses import dataclass

from frce.contracts.evidence_contract import EU_AI_ACT_EVIDENCE_FIELDS


@dataclass(frozen=True)
class EuAiActCheck:
    passed: bool
    missing_fields: list[str]


def check_article_13_fields(fields: list[str]) -> EuAiActCheck:
    missing = sorted(EU_AI_ACT_EVIDENCE_FIELDS - set(fields))
    return EuAiActCheck(passed=not missing, missing_fields=missing)
