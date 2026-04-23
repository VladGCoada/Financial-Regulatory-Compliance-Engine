from __future__ import annotations

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.base_task import BaseTask
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


class SilverCounterpartyTask(BaseTask):
    """
    Upserts counterparty master data into silver.counterparties using SCD2.
    - Incoming record with changed fields: close old row, insert new row
    - Incoming record with no changes: no-op
    Source: moussadiakite SCD2 pattern.
    """

    SCD2_COLS = ["legal_name", "country_code", "risk_tier", "email"]

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_staging(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(f"{self.config.catalog}.bronze.counterparties")
            .withColumn("_source_ingested_at", F.col("ingested_at"))
        )

    def _change_condition(self) -> str:
        return " OR ".join(
            f"existing.{c} <> incoming.{c}" for c in self.SCD2_COLS
        )

    def upsert(self, incoming: DataFrame) -> None:
        target_table = f"{self.config.catalog}.silver.counterparties"
        now = datetime.now(timezone.utc).isoformat()
        spark = self.get_spark()

        # Ensure target table exists
        incoming.limit(0).withColumn("effective_from", F.current_timestamp()) \
            .withColumn("effective_to", F.lit(None).cast("timestamp")) \
            .withColumn("is_current", F.lit(True)) \
            .write.format("delta").mode("ignore").saveAsTable(target_table)

        target = DeltaTable.forName(spark, target_table)

        # Step 1: expire changed rows
        target.alias("existing").merge(
            incoming.alias("incoming"),
            "existing.counterparty_id = incoming.counterparty_id "
            f"AND existing.is_current = true AND ({self._change_condition()})"
        ).whenMatchedUpdate(set={
            "is_current": F.lit(False),
            "effective_to": F.lit(now).cast("timestamp"),
        }).execute()

        # Step 2: insert new/changed rows
        target.alias("existing").merge(
            incoming.alias("incoming"),
            "existing.counterparty_id = incoming.counterparty_id "
            "AND existing.is_current = true"
        ).whenNotMatchedInsert(values={
            "counterparty_id": "incoming.counterparty_id",
            "legal_name": "incoming.legal_name",
            "country_code": "incoming.country_code",
            "iban": "incoming.iban",
            "email": "incoming.email",
            "risk_tier": "incoming.risk_tier",
            "onboarded_at": "incoming.onboarded_at",
            "ingested_at": "incoming.ingested_at",
            "source_file": "incoming.source_file",
            "effective_from": F.lit(now).cast("timestamp"),
            "effective_to": F.lit(None).cast("timestamp"),
            "is_current": F.lit(True),
        }).execute()

    def run(self) -> None:
        df = self.read_staging()
        self.upsert(df)
        logger.info("SilverCounterpartyTask SCD2 upsert complete")