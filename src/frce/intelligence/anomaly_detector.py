from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.base_task import BaseTask
from frce.config import FrceConfig

logger = logging.getLogger(__name__)

FEATURE_COLS = ["amount", "hour_of_day", "day_of_week", "is_cross_border_int"]


class AnomalyDetector(BaseTask):
    """
    Scores SEPA payments using a registered IsolationForest MLflow model.
    Adds columns: anomaly_score (double), is_anomaly (boolean).
    Satisfies DORA Article 9 anomaly detection requirement.
    Source: sarthakmahale storytelling layer elevated to compliance artifact.
    """

    def __init__(self, config: FrceConfig, model_uri: str | None = None) -> None:
        super().__init__(config)
        self.model_uri = model_uri or getattr(config, "anomaly_model_uri", None)

    def engineer_features(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("hour_of_day", F.hour(F.col("booked_at")))
            .withColumn("day_of_week", F.dayofweek(F.col("booked_at")))
            .withColumn(
                "is_cross_border_int",
                F.when(
                    F.col("country_code").isin(["RU", "BY", "IR", "KP", "SY"]), 1
                ).otherwise(0)
            )
        )

    def score(self, df: DataFrame, model: Any) -> DataFrame:
        """
        Apply model UDF to feature-engineered DataFrame.
        model must expose a predict(pandas_df) interface.
        """
        from pyspark.sql.functions import pandas_udf
        import pandas as pd

        @pandas_udf("double")
        def score_udf(*cols) -> pd.Series:
            import numpy as np
            feature_df = pd.concat(list(cols), axis=1)
            feature_df.columns = FEATURE_COLS
            scores = model.decision_function(feature_df.values)
            return pd.Series(scores)

        feature_cols_expr = [F.col(c) for c in FEATURE_COLS]
        return (
            df.withColumn("anomaly_score", score_udf(*feature_cols_expr))
            .withColumn("is_anomaly", F.col("anomaly_score") < -0.1)
        )

    def run(self) -> None:
        raise NotImplementedError(
            "AnomalyDetector.run() requires Databricks + MLflow. "
            "Use engineer_features() + score() directly in pipeline_runner."
        )