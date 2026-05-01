from __future__ import annotations

REQUIRED_FX_COLUMNS = {"rate_id", "base_currency", "target_currency", "rate", "rate_date"}


def validate_fx_columns(columns: list[str]) -> list[str]:
    return sorted(REQUIRED_FX_COLUMNS - set(columns))
