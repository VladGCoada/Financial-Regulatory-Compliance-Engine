from __future__ import annotations

from pyspark.sql import DataFrame


def append_incidents(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("append").saveAsTable(table)
