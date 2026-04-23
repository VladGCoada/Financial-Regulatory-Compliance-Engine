from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _schema_hash(df_schema) -> str:
    schema_str = json.dumps([f.jsonValue() for f in df_schema.fields], sort_keys=True)
    return hashlib.sha256(schema_str.encode()).hexdigest()[:16]


class InferenceLogger:
    """
    Logs every model inference run to gold.mart_model_registry.
    Satisfies EU AI Act Article 13 transparency requirements.
    Fields: model version, run_id, training data version,
            input data version, timestamp, row count, schema hash.
    """

    def __init__(self, spark: Any, catalog: str) -> None:
        self.spark = spark
        self.table = f"{catalog}.gold.mart_model_registry"

    def log(
        self,
        model_name: str,
        model_version: str,
        mlflow_run_id: str,
        training_data_version: int,
        input_data_version: int,
        input_df,
        explainability_sample: dict | None = None,
    ) -> str:
        inference_id = str(uuid.uuid4())
        record = {
            "inference_id": inference_id,
            "model_name": model_name,
            "model_version": model_version,
            "mlflow_run_id": mlflow_run_id,
            "training_data_version": training_data_version,
            "input_data_version": input_data_version,
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "input_row_count": input_df.count(),
            "output_schema_hash": _schema_hash(input_df.schema),
            "explainability_sample": json.dumps(explainability_sample or {}),
        }
        self.spark.createDataFrame([record]) \
            .write.format("delta").mode("append").saveAsTable(self.table)
        logger.info("InferenceLogger: logged inference_id=%s", inference_id)
        return inference_id