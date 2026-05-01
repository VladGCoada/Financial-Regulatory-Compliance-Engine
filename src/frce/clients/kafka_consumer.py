"""Kafka consumer client for FRCE."""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Wrapper for consuming messages from Kafka."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str = "frce-consumer",
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

    def read_stream(
        self,
        spark: SparkSession,
        schema: StructType,
    ) -> Any:
        """
        Read from Kafka as a streaming DataFrame.

        Args:
            spark: SparkSession
            schema: Schema for the value JSON

        Returns:
            Streaming DataFrame
        """
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def read_batch(
        self,
        spark: SparkSession,
        schema: StructType,
        offset: str = "earliest",
    ) -> Any:
        """
        Read from Kafka as a batch DataFrame.

        Args:
            spark: SparkSession
            schema: Schema for the value JSON
            offset: Starting offset ("earliest" or "latest")

        Returns:
            DataFrame
        """
        return (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", offset)
            .load()
        )

    def to_dict_list(self, df: Any, schema: StructType | None = None) -> list[dict[str, Any]]:
        """
        Convert a DataFrame to a list of dictionaries.

        Args:
            df: DataFrame with JSON value column
            schema: Optional schema for parsing JSON payloads

        Returns:
            List of dictionaries
        """
        import json

        from pyspark.sql.functions import col, from_json

        if schema is None:
            rows = df.select(col("value").cast("string").alias("value")).collect()
            return [
                json.loads(row["value"].decode() if isinstance(row["value"], bytes) else row["value"])
                for row in rows
            ]

        parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))
        return [row["data"].asDict() for row in parsed.collect() if row["data"] is not None]
