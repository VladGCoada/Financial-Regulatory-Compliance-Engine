from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.compliance.aml.rule_registry import DEFAULT_AML_RULES, AmlRule

AML_RULES = [
    {
        "flag": rule.flag,
        "condition_sql": rule.condition_sql,
        "risk_score": rule.risk_score,
        "description": rule.description,
        "regulatory_basis": rule.regulatory_basis,
    }
    for rule in DEFAULT_AML_RULES
]


class AmlRuleEngine:
    def __init__(self, rules: list[AmlRule] | None = None) -> None:
        self.rules = rules if rules is not None else DEFAULT_AML_RULES

    def apply(self, df: DataFrame) -> DataFrame:
        if not self.rules:
            return (
                df.withColumn("aml_flags", F.array())
                .withColumn("max_risk_score", F.lit(0.0))
                .withColumn("is_flagged", F.lit(False))
            )

        for rule in self.rules:
            df = df.withColumn(
                f"_flag_{rule.flag}",
                F.when(F.expr(rule.condition_sql), F.lit(rule.risk_score)).otherwise(
                    F.lit(None).cast("double")
                ),
            )

        flag_cols = [f"_flag_{rule.flag}" for rule in self.rules]
        flag_names = [
            F.when(F.col(col).isNotNull(), F.lit(col.replace("_flag_", ""))).otherwise(
                F.lit(None).cast("string")
            )
            for col in flag_cols
        ]
        scores = [F.coalesce(F.col(col), F.lit(0.0)) for col in flag_cols]

        return (
            df.withColumn("aml_flags_raw", F.array(*flag_names))
            .withColumn("aml_flags", F.expr("filter(aml_flags_raw, x -> x is not null)"))
            .withColumn("max_risk_score", F.greatest(*scores))
            .withColumn("is_flagged", F.size(F.col("aml_flags")) > 0)
            .drop("aml_flags_raw", *flag_cols)
        )

    def rule_names(self) -> list[str]:
        return [rule.flag for rule in self.rules]

    def describe(self) -> list[dict]:
        return [
            {
                "flag": rule.flag,
                "risk_score": rule.risk_score,
                "description": rule.description,
                "regulatory_basis": rule.regulatory_basis,
            }
            for rule in self.rules
        ]


AMLRuleEngine = AmlRuleEngine
