from __future__ import annotations

from enum import Enum


class Environment(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


def catalog_for_environment(environment: str) -> str:
    return {
        Environment.LOCAL.value: "frce_local",
        Environment.DEV.value: "frce_dev",
        Environment.STAGING.value: "frce_staging",
        Environment.PROD.value: "frce_prod",
    }.get(environment, "frce_dev")
