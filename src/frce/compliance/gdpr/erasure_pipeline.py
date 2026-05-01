from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame

from frce.core import BaseTask

logger = logging.getLogger(__name__)


class GdprErasurePipeline(BaseTask):
    def load_pending_requests(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(self.config.compliance_erasure_requests_table)
            .filter("status = 'PENDING'")
        )

    def erase_from_bronze(self, entity_id: str, entity_type: str) -> int:
        col = "debtor_iban" if entity_type == "DEBTOR_IBAN" else "creditor_iban"
        return self._delete_where(self.config.payments_bronze_table, col, entity_id)

    def erase_from_silver(self, entity_id: str, entity_type: str) -> int:
        col = "debtor_iban" if entity_type == "DEBTOR_IBAN" else "creditor_iban"
        return self._delete_where(self.config.silver_payments_table, col, entity_id)

    def erase_from_gold(self, entity_id: str, entity_type: str) -> int:
        col = "debtor_iban_hash" if entity_type == "DEBTOR_IBAN" else "creditor_iban_hash"
        entity_hash = hashlib.sha256(entity_id.encode("utf-8")).hexdigest()
        return self._delete_where(self.config.gold_fact_payments_table, col, entity_hash)

    def mark_request_complete(
        self,
        request_id: str,
        bronze_rows: int,
        silver_rows: int,
        gold_rows: int,
    ) -> None:
        record = {
            "audit_id": str(uuid.uuid4()),
            "request_id": request_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "bronze_rows_deleted": bronze_rows,
            "silver_rows_deleted": silver_rows,
            "gold_rows_deleted": gold_rows,
            "operator": "gdpr_erasure_pipeline",
        }
        spark = self.get_spark()
        spark.createDataFrame([record]).write.format("delta").mode("append").saveAsTable(
            self.config.audit_erasure_audit_table
        )
        spark.sql(
            f"UPDATE {self.config.compliance_erasure_requests_table} "
            f"SET status = 'COMPLETED' WHERE request_id = '{request_id}'"
        )

    def run(self) -> None:
        pending = self.load_pending_requests().collect()
        for row in pending:
            bronze = self.erase_from_bronze(row.entity_id, row.entity_type)
            silver = self.erase_from_silver(row.entity_id, row.entity_type)
            gold = self.erase_from_gold(row.entity_id, row.entity_type)
            self.mark_request_complete(row.request_id, bronze, silver, gold)
        logger.info("GDPR erasure complete: %d requests processed", len(pending))

    def _delete_where(self, table: str, column: str, value: str) -> int:
        result = self.get_spark().sql(f"DELETE FROM {table} WHERE {column} = '{value}'")
        rows = result.collect() if result is not None else []
        return int(rows[0]["num_deleted_rows"]) if rows and "num_deleted_rows" in rows[0] else 0


GDPRErasurePipeline = GdprErasurePipeline
