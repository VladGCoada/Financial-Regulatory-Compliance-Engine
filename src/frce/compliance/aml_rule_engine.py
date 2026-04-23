from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


AML_RULES = [
    {
        "flag": "HIGH_VALUE_SINGLE_TXN",
        "condition_sql": "amount > 100000",
        "risk_score": 0.6,
        "description": "Single transaction > EUR 100,000",
    },
    {
        "flag": "HIGH_RISK_JURISDICTION",
        "condition_sql": "country_code IN ('RU','BY','IR','KP','SY') AND amount > 1000",
        "risk_score": 0.9,
        "description": "Transaction involving high-risk jurisdiction",
    },
    {
        "flag": "ROUND_AMOUNT",
        "condition_sql": "amount % 10000 = 0 AND amount >= 10000",
        "risk_score": 0.3,
        "description": "Suspiciously round large amount",
    },
]


class AmlRuleEngine:
    def apply(self, df: DataFrame) -> DataFrame:
        for rule in AML_RULES:
            df = df.withColumn(
                f"_flag_{rule['flag']}",
                F.when(F.expr(rule["condition_sql"]), F.lit(rule["risk_score"])).otherwise(F.lit(None)),
            )

        flag_cols = [f"_flag_{r['flag']}" for r in AML_RULES]
        flag_name_exprs = [
            F.when(F.col(c).isNotNull(), F.lit(c.replace("_flag_", ""))).otherwise(F.lit(None))
            for c in flag_cols
        ]

        df = df.withColumn("aml_flags_raw", F.array(*flag_name_exprs))
        df = df.withColumn("aml_flags", F.expr("filter(aml_flags_raw, x -> x is not null)"))

        score_exprs = [F.coalesce(F.col(c), F.lit(0.0)) for c in flag_cols]

        df = (
            df.withColumn("max_risk_score", F.greatest(*score_exprs))
            .withColumn("is_flagged", F.size(F.col("aml_flags")) > 0)
            .drop("aml_flags_raw", *flag_cols)
        )
        return df