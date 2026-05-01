from __future__ import annotations


def should_open_incident(failed_tasks: int, critical_failures: int = 0) -> bool:
    return critical_failures > 0 or failed_tasks >= 3
