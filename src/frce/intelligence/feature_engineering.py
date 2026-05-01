from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_payment_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("hour_of_day", F.hour(F.col("booked_at")))
        .withColumn("day_of_week", F.dayofweek(F.col("booked_at")))
        .withColumn(
            "is_cross_border_int",
            F.when(F.col("country_code").isin(["RU", "BY", "IR", "KP", "SY"]), 1).otherwise(0),
        )
    )
