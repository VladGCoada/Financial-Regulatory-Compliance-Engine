from __future__ import annotations

from frce.compliance.aml.rule_engine import AmlRuleEngine


def benchmark_rule_count() -> int:
    return len(AmlRuleEngine().rule_names())
