from __future__ import annotations

TASK_DEPENDENCIES: dict[str, list[str]] = {
    "silver_payments": [],
    "silver_counterparty": [],
    "silver_fx": [],
    "fact_payments": ["silver_payments"],
    "dim_counterparty": ["silver_counterparty"],
    "mart_aml_alerts": ["fact_payments"],
    "mart_dora_incidents": ["silver_payments"],
    "mart_gdpr_requests": ["fact_payments"],
}

TASK_ORDER = [
    "silver_payments",
    "silver_counterparty",
    "silver_fx",
    "fact_payments",
    "dim_counterparty",
    "mart_aml_alerts",
    "mart_dora_incidents",
    "mart_gdpr_requests",
]


def dependencies_for(task_name: str) -> list[str]:
    return TASK_DEPENDENCIES.get(task_name, [])


def validate_task_order(task_order: list[str] = TASK_ORDER) -> bool:
    positions = {task: index for index, task in enumerate(task_order)}
    return all(
        positions[dependency] < positions[task]
        for task, dependencies in TASK_DEPENDENCIES.items()
        if task in positions
        for dependency in dependencies
        if dependency in positions
    )
