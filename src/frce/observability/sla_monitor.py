from __future__ import annotations


def is_fresh(age_minutes: float, freshness_minutes: float) -> bool:
    return age_minutes <= freshness_minutes
