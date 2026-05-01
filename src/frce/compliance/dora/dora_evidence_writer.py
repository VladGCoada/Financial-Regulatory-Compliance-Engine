from __future__ import annotations

from pathlib import Path


def write_dora_evidence_markdown(path: str | Path, incidents: list[dict]) -> None:
    lines = ["# DORA Incident Evidence", ""]
    lines.extend(f"- `{incident.get('run_id')}` severity={incident.get('severity')}" for incident in incidents)
    Path(path).write_text("\n".join(lines), encoding="utf-8")
