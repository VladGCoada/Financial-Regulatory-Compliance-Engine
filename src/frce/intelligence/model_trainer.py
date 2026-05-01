from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TrainingResult:
    model_name: str
    model_version: str
    metrics: dict[str, float]
    artifact_uri: str | None = None


class ModelTrainer:
    """
    Local-safe model trainer adapter.

    Production training is expected to run on Databricks/MLflow; this class
    captures the contract used by orchestration without pulling those heavy
    services into unit tests.
    """

    def train(self, training_data: Any, model_name: str = "frce_anomaly_detector") -> TrainingResult:
        row_count = training_data.count() if hasattr(training_data, "count") else 0
        return TrainingResult(
            model_name=model_name,
            model_version="local",
            metrics={"training_rows": float(row_count)},
            artifact_uri=None,
        )
