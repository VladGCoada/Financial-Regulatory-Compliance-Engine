from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricPoint:
    name: str
    value: float
    tags: dict[str, str]


def metric(name: str, value: float, **tags: str) -> MetricPoint:
    return MetricPoint(name=name, value=value, tags=tags)
