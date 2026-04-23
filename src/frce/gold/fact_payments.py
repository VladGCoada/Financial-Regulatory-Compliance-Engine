from __future__ import annotations

import hashlib
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from frce.base_task import BaseTask
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


def _sha256_udf(col_name: str) -> F.Column:
    """Hash a column value using SHA-256 (Spark built-in — no Python UDF needed)."""
    return F.sha2(F.col(col_name), 256)


class FactPaymentsTask(BaseTask):
    """
    Builds gold.fact_payments from silver.payments + AML flags.
    PII masking: IBAN stored as SHA-256 hash only.
    Actual masking at query time handled by UC ABAC (abac_policies.tf).
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_silver(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(f"{self.config.catalog}.silver.payments")
        )

    def mask_pii(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("debtor_iban_hash", _sha256_udf("debtor_iban"))
            .withColumn("creditor_iban_hash", _sha256_udf("creditor_iban"))
            .drop("debtor_iban", "creditor_iban",
                  "debtor_name", "creditor_name",
                  "debtor_email", "creditor_email")
        )

    def write_gold(self, df: DataFrame) -> None:
        target = f"{self.config.catalog}.gold.fact_payments"
        df.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true").saveAsTable(target)

    def run(self) -> None:
        df = self.read_silver()
        df = self.mask_pii(df)
        self.write_gold(df)
        logger.info("FactPaymentsTask complete")