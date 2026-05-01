from __future__ import annotations

import hashlib


def sha256_hex(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
