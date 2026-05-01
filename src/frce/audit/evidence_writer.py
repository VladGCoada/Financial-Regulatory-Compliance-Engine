from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path


def write_markdown_evidence(path: str | Path, title: str, rows: Iterable[dict]) -> None:
    lines = [f"# {title}", ""]
    for row in rows:
        lines.append(f"- {row}")
    Path(path).write_text("\n".join(lines), encoding="utf-8")
