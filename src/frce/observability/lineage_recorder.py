from __future__ import annotations

from frce.audit.lineage_events import LineageEvent, build_lineage_event


def record_lineage(task_name: str, inputs: list[str], outputs: list[str]) -> LineageEvent:
    return build_lineage_event(task_name, inputs, outputs)
