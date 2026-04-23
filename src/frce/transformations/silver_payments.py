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
    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)
        self.validator = DQValidator(PAYMENT_DQ_RULES)

    def read_bronze(self) -> DataFrame:
        return self.get_spark().read.format("delta").table(self.config.payments_bronze_table)

    def cast_and_clean(self, df: DataFrame) -> DataFrame:
        if "amount" in df.columns:
            df = df.withColumn("amount", F.col("amount").cast("double"))
        if "booked_at" in df.columns:
            df = df.withColumn("booked_at", F.col("booked_at").cast("timestamp"))
        if "ingested_at" in df.columns:
            df = df.withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
        if "country_code" in df.columns:
            df = df.withColumn("country_code", F.upper(F.trim(F.col("country_code"))))
        if "currency" in df.columns:
            df = df.withColumn("currency", F.upper(F.trim(F.col("currency"))))
        return df

    def write_silver(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(
            self.config.silver_payments_table
        )

    def write_quarantine(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").saveAsTable(self.config.quarantine_payments_table)

    def run(self) -> None:
        df = self.read_bronze()
        df = self.cast_and_clean(df)
        result = self.validator.validate(df)
        self.write_silver(result.good)
        if result.total_quarantine > 0:
            self.write_quarantine(result.quarantine)
        self.logger.info(
            "SilverPayments: %d good / %d quarantined",
            result.total_good,
            result.total_quarantine,
        )