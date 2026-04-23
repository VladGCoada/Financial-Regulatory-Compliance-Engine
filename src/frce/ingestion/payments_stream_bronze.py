from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from frce.base_task import BaseTask
from frce.common_schemas import SEPA_PAYMENT_SCHEMA
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


class PaymentsStreamBronzeTask(BaseTask):
    """
    Reads SEPA payment events from Kafka and appends to bronze.raw_payments.
    Uses Auto Loader (cloudFiles) on Databricks for production streaming.
    Source: moussadiakite Bronze append-only semantics.
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_stream(self) -> DataFrame:
        """
        Production: reads from ADLS landing zone via Auto Loader.
        Returns a streaming DataFrame.
        """
        spark = self.get_spark()
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", self.config.payments_schema_location)
            .schema(SEPA_PAYMENT_SCHEMA)
            .load(self.config.payments_landing_path)
        )

    def add_metadata(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_system", F.coalesce(F.col("source_system"), F.lit("UNKNOWN")))
        )

    def write_stream(self, df: DataFrame) -> None:
        target = self.config.payments_bronze_table
        checkpoint = self.config.payments_checkpoint_path
        (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "false")
            .toTable(target)
        )

    def run(self) -> None:
        df = self.read_stream()
        df = self.add_metadata(df)
        self.write_stream(df)
        logger.info("PaymentsStreamBronzeTask started")