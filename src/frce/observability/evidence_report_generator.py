from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_evidence_json(path: str | Path, payload: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
