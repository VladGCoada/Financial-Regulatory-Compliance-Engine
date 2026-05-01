from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.compliance.aml_rule_engine import AmlRuleEngine
from frce.config import FrceConfig
from frce.core import BaseTask

logger = logging.getLogger(__name__)


class MartAmlAlertsTask(BaseTask):
    """
    Builds gold.mart_aml_alerts: flagged payments with AML risk scores.
    Input: silver.payments
    Output: gold.mart_aml_alerts (flagged rows only)
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)
        self.aml = AmlRuleEngine()

    def read_silver(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(self.config.silver_payments_table)
        )

    def build(self, df: DataFrame) -> DataFrame:
        scored = self.aml.apply(df)
        return (
            scored.filter(F.col("is_flagged"))
            .withColumn("alert_created_at", F.current_timestamp())
            .select(
                "payment_id", "transaction_reference",
                "amount", "currency", "country_code",
                "booked_at", "aml_flags", "max_risk_score",
                "is_flagged", "alert_created_at"
            )
        )

    def run(self) -> None:
        df = self.read_silver()
        alerts = self.build(df)
        target = self.config.gold_mart_aml_alerts_table
        alerts.write.format("delta").mode("overwrite").saveAsTable(target)
        logger.info("MartAmlAlertsTask: wrote %d alerts", alerts.count())
