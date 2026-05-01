from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class LineageEvent:
    task_name: str
    input_tables: list[str]
    output_tables: list[str]
    event_time: str


def build_lineage_event(
    task_name: str,
    input_tables: list[str],
    output_tables: list[str],
) -> LineageEvent:
    return LineageEvent(
        task_name=task_name,
        input_tables=input_tables,
        output_tables=output_tables,
        event_time=datetime.now(timezone.utc).isoformat(),
    )
