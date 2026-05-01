from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.compliance.aml.rule_engine import AmlRuleEngine


def build_aml_alerts(df: DataFrame) -> DataFrame:
    scored = AmlRuleEngine().apply(df)
    return (
        scored.filter(F.col("is_flagged") == F.lit(True))
        .withColumn("alert_created_at", F.current_timestamp())
        .select(
            "payment_id",
            "transaction_reference",
            "amount",
            "currency",
            "country_code",
            "booked_at",
            "aml_flags",
            "max_risk_score",
            "is_flagged",
            "alert_created_at",
        )
    )
