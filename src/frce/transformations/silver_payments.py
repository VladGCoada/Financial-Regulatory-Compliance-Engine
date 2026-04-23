from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.base_task import BaseTask
from frce.config import FrceConfig
from frce.quality.dq_rules import PAYMENT_DQ_RULES
from frce.quality.dq_validator import DQValidator

logger = logging.getLogger(__name__)


class SilverPaymentsTask(BaseTask):
    """
    Reads bronze.raw_payments, validates DQ rules, routes bad rows
    to bronze.quarantine_payments, writes clean rows to silver.payments.
    Source: moussadiakite quarantine + SCD2 patterns.
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)
        self.validator = DQValidator(PAYMENT_DQ_RULES)

    def read_bronze(self) -> DataFrame:
        return self.get_spark().read.format("delta").table(
            f"{self.config.catalog}.bronze.raw_payments"
        )

    def cast_and_clean(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("amount", F.col("amount").cast("double"))
            .withColumn("booked_at", F.col("booked_at").cast("timestamp"))
            .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
            .withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
            .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        )

    def write_silver(self, df: DataFrame) -> None:
        target = f"{self.config.catalog}.silver.payments"
        df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(target)

    def write_quarantine(self, df: DataFrame) -> None:
        target = f"{self.config.catalog}.bronze.quarantine_payments"
        df.write.format("delta").mode("append").saveAsTable(target)

    def run(self) -> None:
        df = self.read_bronze()
        df = self.cast_and_clean(df)
        result = self.validator.validate(df)
        self.write_silver(result.good)
        if result.total_quarantine > 0:
            self.write_quarantine(result.quarantine)
        logger.info(
            "SilverPayments: %d good / %d quarantined",
            result.total_good, result.total_quarantine,
        )