from __future__ import annotations

EU_AI_ACT_EVIDENCE_FIELDS = {
    "inference_id",
    "model_name",
    "model_version",
    "mlflow_run_id",
    "training_data_version",
    "input_data_version",
    "scored_at",
    "input_row_count",
    "output_schema_hash",
    "explainability_sample",
}


def missing_model_evidence_fields(fields: list[str]) -> list[str]:
    return sorted(EU_AI_ACT_EVIDENCE_FIELDS - set(fields))
