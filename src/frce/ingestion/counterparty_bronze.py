from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.base_task import BaseTask
from frce.common_schemas import COUNTERPARTY_SCHEMA
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


class CounterpartyBronzeTask(BaseTask):
    """
    Reads counterparty CSV from ADLS and appends to bronze.counterparties.
    Append-only: never overwrites existing rows (Bronze immutability contract).
    Source: moussadiakite Bronze append-only semantics.
    """

    def __init__(self, config: FrceConfig, source_path: str | None = None) -> None:
        super().__init__(config)
        self.source_path = source_path or config.counterparty_source_path

    def read_csv(self) -> DataFrame:
        return (
            self.get_spark()
            .read.option("header", "true")
            .option("inferSchema", "false")
            .schema(COUNTERPARTY_SCHEMA)
            .csv(self.source_path)
        )

    def add_metadata(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "ingested_at",
            F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"),
        ).withColumn(
            "source_file",
            F.input_file_name(),
        )

    def write_bronze(self, df: DataFrame) -> None:
        target = self.config.counterparty_bronze_table
        logger.info("Writing %d rows to %s", df.count(), target)
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(target)
        )

    def run(self) -> None:
        df = self.read_csv()
        df = self.add_metadata(df)
        self.write_bronze(df)
        logger.info("CounterpartyBronzeTask complete")