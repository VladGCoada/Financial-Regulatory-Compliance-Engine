from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


AML_RULES = [
    {
        "flag": "HIGH_VALUE_SINGLE_TXN",
        "condition": F.col("amount") > 100_000,
        "risk_score": 0.6,
        "description": "Single transaction > EUR 100,000",
    },
    {
        "flag": "CROSS_BORDER_HIGH_RISK",
        "condition": (F.col("country_code").isin(["RU", "BY", "IR", "KP", "SY"]))
                     & (F.col("amount") > 1_000),
        "risk_score": 0.9,
        "description": "Cross-border to sanctioned jurisdiction",
    },
    {
        "flag": "ROUND_AMOUNT",
        "condition": (F.col("amount") % 10_000 == 0) & (F.col("amount") >= 10_000),
        "risk_score": 0.3,
        "description": "Suspiciously round large amount",
    },
]


class AmlRuleEngine:
    """
    Applies rule-based AML flags to a Silver payments DataFrame.
    Returns original DataFrame with added columns:
      aml_flags (array<string>), max_risk_score (double), is_flagged (boolean)
    """

    def apply(self, df: DataFrame) -> DataFrame:
        for rule in AML_RULES:
            df = df.withColumn(
                f"_flag_{rule['flag']}",
                F.when(rule["condition"], rule["risk_score"]).otherwise(None),
            )

        flag_cols = [f"_flag_{r['flag']}" for r in AML_RULES]
        flag_name_cols = [
            F.when(F.col(c).isNotNull(), F.lit(c.replace("_flag_", "")))
            for c in flag_cols
        ]

        df = (
            df.withColumn("aml_flags", F.array_compact(F.array(*flag_name_cols)))
            .withColumn("max_risk_score", F.greatest(*[F.coalesce(F.col(c), F.lit(0.0)) for c in flag_cols]))
            .withColumn("is_flagged", F.size(F.col("aml_flags")) > 0)
            .drop(*flag_cols)
        )
        return df