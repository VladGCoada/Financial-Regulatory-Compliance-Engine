from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.core import BaseTask


class SilverCounterpartyTask(BaseTask):
    """
    Maintains silver counterparty master data with SCD2 semantics.

    Local unit tests import this without requiring delta-spark. In Databricks,
    upsert() uses DeltaTable when available; otherwise it appends prepared rows
    as a local-safe fallback for development.
    """

    SCD2_COLS = ["legal_name", "country_code", "risk_tier", "email"]

    def read_staging(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(self.config.counterparty_bronze_table)
            .withColumn("_source_ingested_at", F.col("ingested_at"))
        )

    def _change_condition(self) -> str:
        return " OR ".join(f"existing.{column} <> incoming.{column}" for column in self.SCD2_COLS)

    def prepare_incoming(self, incoming: DataFrame) -> DataFrame:
        return (
            incoming.withColumn("effective_from", F.current_timestamp())
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

    def upsert(self, incoming: DataFrame) -> None:
        target_table = self.config.silver_counterparty_table
        spark = self.get_spark()

        try:
            from delta.tables import DeltaTable
        except ImportError:
            self.prepare_incoming(incoming).write.format("delta").mode("append").saveAsTable(target_table)
            return

        self.prepare_incoming(incoming.limit(0)).write.format("delta").mode("ignore").saveAsTable(
            target_table
        )
        now = datetime.now(timezone.utc).isoformat()
        target = DeltaTable.forName(spark, target_table)

        target.alias("existing").merge(
            incoming.alias("incoming"),
            "existing.counterparty_id = incoming.counterparty_id "
            f"AND existing.is_current = true AND ({self._change_condition()})",
        ).whenMatchedUpdate(
            set={
                "is_current": F.lit(False),
                "effective_to": F.lit(now).cast("timestamp"),
            }
        ).execute()

        target.alias("existing").merge(
            incoming.alias("incoming"),
            "existing.counterparty_id = incoming.counterparty_id AND existing.is_current = true",
        ).whenNotMatchedInsert(
            values={
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
            }
        ).execute()

    def run(self) -> None:
        self.upsert(self.read_staging())
