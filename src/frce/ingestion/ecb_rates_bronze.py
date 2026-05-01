from __future__ import annotations

from datetime import date
from typing import Any

from pyspark.sql import DataFrame

from frce.clients.ecb_client import EcbClient
from frce.common_schemas import FX_RATE_SCHEMA
from frce.core import BaseTask


class EcbRatesBronzeTask(BaseTask):
    DEFAULT_CURRENCIES = ["USD", "GBP", "CHF", "SEK", "DKK", "PLN", "CZK"]

    def __init__(
        self,
        config=None,
        currencies: list[str] | None = None,
        ref_date: date | None = None,
        client: EcbClient | None = None,
    ) -> None:
        super().__init__(config)
        self.currencies = currencies or self.DEFAULT_CURRENCIES
        self.ref_date = ref_date
        self.client = client or EcbClient(base_url=self.config.ecb_base_url)

    def to_dataframe(self, rows: list[dict[str, Any]]) -> DataFrame:
        return self.get_spark().createDataFrame(rows, schema=FX_RATE_SCHEMA)

    def write_bronze(self, df: DataFrame) -> None:
        df.write.format("delta").mode("append").saveAsTable(self.config.fx_rates_bronze_table)

    def run(self) -> None:
        rows = self.client.get_rates(self.currencies, self.ref_date)
        if rows:
            self.write_bronze(self.to_dataframe(rows))


ECBRatesBronzeTask = EcbRatesBronzeTask
