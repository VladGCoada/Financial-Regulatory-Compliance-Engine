from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from frce.config.settings import FrceConfig
from frce.utils.spark import get_spark


class BaseTask(ABC):
    def __init__(self, config: FrceConfig | None = None) -> None:
        self.config = config or FrceConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._spark: SparkSession | None = None

    def get_spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = get_spark(self.__class__.__name__)
        return self._spark

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
