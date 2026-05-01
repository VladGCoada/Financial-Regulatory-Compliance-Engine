from __future__ import annotations


def dq_rate(total_in: int, total_quarantine: int) -> float:
    if total_in == 0:
        return 0.0
    return total_quarantine / total_in
