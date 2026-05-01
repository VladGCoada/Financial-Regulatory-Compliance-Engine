from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quarantine_metadata(df: DataFrame, reason_col: str = "_dq_failure_reason") -> DataFrame:
    return df.withColumn("quarantine_reason", F.col(reason_col)).withColumn(
        "quarantine_at", F.current_timestamp()
    )
