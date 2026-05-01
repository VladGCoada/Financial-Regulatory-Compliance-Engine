from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HealthCheckResult:
    name: str
    healthy: bool
    message: str


def table_readable(name: str, readable: bool) -> HealthCheckResult:
    return HealthCheckResult(name=name, healthy=readable, message="ok" if readable else "unreadable")
