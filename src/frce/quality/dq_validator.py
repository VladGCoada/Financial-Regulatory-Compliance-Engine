from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    good: DataFrame
    quarantine: DataFrame
    total_in: int
    total_good: int
    total_quarantine: int


class DQValidator:
    """
    Applies a list of DQ rules to a DataFrame.
    Rows that fail any rule go to quarantine with the failure reason attached.
    Source: moussadiakite quarantine table pattern.
    """

    def __init__(self, rules: list[dict]) -> None:
        self.rules = rules

    def validate(self, df: DataFrame) -> DQResult:
        # Build a combined pass/fail column per rule
        fail_conditions = []
        for rule in self.rules:
            fail_col = F.when(~F.expr(rule["condition_sql"]), rule["quarantine_reason"])
            fail_conditions.append(fail_col)

        # First failure reason wins (coalesce)
        df = df.withColumn("_dq_failure_reason", F.coalesce(*fail_conditions))

        good = df.filter(F.col("_dq_failure_reason").isNull()).drop("_dq_failure_reason")
        quarantine = df.filter(F.col("_dq_failure_reason").isNotNull()).withColumn(
            "quarantine_at", F.current_timestamp()
        )

        total_in = df.count()
        total_good = good.count()
        total_quarantine = quarantine.count()

        logger.info(
            "DQ: %d in / %d good / %d quarantined",
            total_in, total_good, total_quarantine,
        )
        return DQResult(good, quarantine, total_in, total_good, total_quarantine)