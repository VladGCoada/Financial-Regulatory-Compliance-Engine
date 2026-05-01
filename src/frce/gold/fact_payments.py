from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.config import FrceConfig
from frce.core import BaseTask

logger = logging.getLogger(__name__)


def sha2_col(col_name: str) -> F.Column:
    return F.sha2(F.col(col_name), 256)


_sha256_udf = sha2_col


class FactPaymentsTask(BaseTask):
    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_silver(self) -> DataFrame:
        return self.get_spark().read.format("delta").table(self.config.silver_payments_table)

    def mask_pii(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("debtor_iban_hash", sha2_col("debtor_iban"))
            .withColumn("creditor_iban_hash", sha2_col("creditor_iban"))
            .drop(
                "debtor_iban",
                "creditor_iban",
                "debtor_name",
                "creditor_name",
                "debtor_email",
                "creditor_email",
            )
        )

    def write_gold(self, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            self.config.gold_fact_payments_table
        )

    def run(self) -> None:
        self.write_gold(self.mask_pii(self.read_silver()))
        logger.info("FactPaymentsTask complete")
