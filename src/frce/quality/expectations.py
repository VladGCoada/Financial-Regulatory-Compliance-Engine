from __future__ import annotations


def expect_columns(columns: list[str], required: set[str]) -> list[str]:
    return sorted(required - set(columns))
