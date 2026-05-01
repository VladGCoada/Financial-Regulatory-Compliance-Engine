from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.config import FrceConfig
from frce.core import BaseTask

logger = logging.getLogger(__name__)


class MartGdprRequestsTask(BaseTask):
    """
    Builds gold.mart_gdpr_requests from compliance.erasure_requests
    joined with audit.erasure_audit.
    Provides a complete right-to-erasure audit trail for regulators.
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_requests(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(self.config.compliance_erasure_requests_table)
        )

    def read_audit(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(self.config.audit_erasure_audit_table)
        )

    def build(self, requests: DataFrame, audit: DataFrame) -> DataFrame:
        return (
            requests.alias("req")
            .join(audit.alias("aud"), "request_id", "left")
            .select(
                F.col("req.request_id"),
                F.col("req.entity_type"),
                F.col("req.entity_id"),
                F.col("req.requested_at"),
                F.col("req.status"),
                F.col("aud.completed_at"),
                F.col("aud.bronze_rows_deleted"),
                F.col("aud.silver_rows_deleted"),
                F.col("aud.gold_rows_deleted"),
                F.col("aud.operator"),
            )
        )

    def run(self) -> None:
        requests = self.read_requests()
        audit = self.read_audit()
        mart = self.build(requests, audit)
        target = self.config.gold_mart_gdpr_requests_table
        mart.write.format("delta").mode("overwrite").saveAsTable(target)
        logger.info("MartGdprRequestsTask complete")
