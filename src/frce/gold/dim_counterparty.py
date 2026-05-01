from __future__ import annotations

from frce.core import BaseTask


class DimCounterpartyTask(BaseTask):
    def run(self) -> None:
        df = self.get_spark().read.format("delta").table(self.config.silver_counterparty_table)
        df.filter("is_current = true").write.format("delta").mode("overwrite").saveAsTable(
            self.config.gold_dim_counterparty_table
        )
