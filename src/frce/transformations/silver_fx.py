from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from frce.base_task import BaseTask
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


class SilverFxTask(BaseTask):
    """
    Cleans bronze.fx_rates into silver.fx_rates.
    - Deduplicates by (target_currency, rate_date), keeping latest ingested
    - Filters out null rates and non-EUR base
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_bronze(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(f"{self.config.catalog}.bronze.fx_rates")
        )

    def clean(self, df: DataFrame) -> DataFrame:
        window = Window.partitionBy("target_currency", "rate_date") \
                       .orderBy(F.col("ingested_at").desc())

        return (
            df.filter(F.col("base_currency") == "EUR")
            .filter(F.col("rate").isNotNull())
            .filter(F.col("rate") > 0)
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

    def write_silver(self, df: DataFrame) -> None:
        target = f"{self.config.catalog}.silver.fx_rates"
        df.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true").saveAsTable(target)

    def run(self) -> None:
        df = self.read_bronze()
        clean_df = self.clean(df)
        self.write_silver(clean_df)
        logger.info("SilverFxTask complete")