from __future__ import annotations

from frce.core import BaseTask


class MartModelRegistryTask(BaseTask):
    def run(self) -> None:
        self.logger.info(
            "mart_model_registry is populated by InferenceLogger at %s",
            self.config.gold_mart_model_registry_table,
        )
