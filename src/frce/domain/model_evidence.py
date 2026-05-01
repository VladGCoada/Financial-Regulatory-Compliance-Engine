from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelInferenceEvidence:
    inference_id: str
    model_name: str
    model_version: str
    mlflow_run_id: str
    training_data_version: int
    input_data_version: int
    input_row_count: int
    output_schema_hash: str
