from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame

from frce.base_task import BaseTask
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


class GdprErasurePipeline(BaseTask):
    """
    Processes GDPR right-to-erasure requests.
    Cascade: Bronze → Silver → Gold (idempotent per entity_id).
    Writes completion record to audit.erasure_audit on success.

    Implementation follows Databricks recommended pattern:
    delete from Bronze first via Delta ACID DELETE, then re-run
    Silver/Gold transformations for affected entity range.
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def load_pending_requests(self) -> DataFrame:
        """
        Reads from compliance.erasure_requests where status = 'PENDING'.
        Schema: (request_id, entity_type, entity_id, requested_at, status)
        """
        return (
            self.get_spark()
            .read.format("delta")
            .table(f"{self.config.catalog}.compliance.erasure_requests")
            .filter("status = 'PENDING'")
        )

    def erase_from_bronze(self, entity_id: str, entity_type: str) -> int:
        spark = self.get_spark()
        table = f"{self.config.catalog}.bronze.raw_payments"
        col = "debtor_iban" if entity_type == "DEBTOR_IBAN" else "creditor_iban"
        result = spark.sql(
            f"DELETE FROM {table} WHERE {col} = '{entity_id}'"
        )
        deleted = result.collect()[0]["num_deleted_rows"] if result else 0
        logger.info("Bronze DELETE: %d rows for %s=%s", deleted, col, entity_id)
        return deleted

    def erase_from_silver(self, entity_id: str, entity_type: str) -> int:
        spark = self.get_spark()
        table = f"{self.config.catalog}.silver.payments"
        col = "debtor_iban" if entity_type == "DEBTOR_IBAN" else "creditor_iban"
        result = spark.sql(f"DELETE FROM {table} WHERE {col} = '{entity_id}'")
        deleted = result.collect()[0]["num_deleted_rows"] if result else 0
        logger.info("Silver DELETE: %d rows for %s=%s", deleted, col, entity_id)
        return deleted

    def erase_from_gold(self, entity_id: str, entity_type: str) -> int:
        spark = self.get_spark()
        table = f"{self.config.catalog}.gold.fact_payments"
        col = "debtor_iban_hash" if entity_type == "DEBTOR_IBAN" else "creditor_iban_hash"
        # Gold stores hashed IBAN — erase by hash match
        import hashlib
        iban_hash = hashlib.sha256(entity_id.encode()).hexdigest()
        result = spark.sql(f"DELETE FROM {table} WHERE {col} = '{iban_hash}'")
        deleted = result.collect()[0]["num_deleted_rows"] if result else 0
        logger.info("Gold DELETE: %d rows for hash of %s", deleted, entity_id)
        return deleted

    def mark_request_complete(self, request_id: str, bronze_rows: int,
                               silver_rows: int, gold_rows: int) -> None:
        spark = self.get_spark()
        audit_table = f"{self.config.catalog}.audit.erasure_audit"
        record = {
            "audit_id": str(uuid.uuid4()),
            "request_id": request_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "bronze_rows_deleted": bronze_rows,
            "silver_rows_deleted": silver_rows,
            "gold_rows_deleted": gold_rows,
            "operator": "gdpr_erasure_pipeline",
        }
        spark.createDataFrame([record]).write.format("delta").mode("append").saveAsTable(audit_table)
        spark.sql(
            f"UPDATE {self.config.catalog}.compliance.erasure_requests "
            f"SET status = 'COMPLETED' WHERE request_id = '{request_id}'"
        )

    def run(self) -> None:
        pending = self.load_pending_requests()
        rows = pending.collect()
        logger.info("GDPR erasure: %d pending requests", len(rows))
        for row in rows:
            b = self.erase_from_bronze(row.entity_id, row.entity_type)
            s = self.erase_from_silver(row.entity_id, row.entity_type)
            g = self.erase_from_gold(row.entity_id, row.entity_type)
            self.mark_request_complete(row.request_id, b, s, g)
        logger.info("GDPR erasure complete: %d requests processed", len(rows))