from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from frce.config import FrceConfig


class BaseTask(ABC):
    def __init__(self, config: FrceConfig | None = None) -> None:
        self.config = config or FrceConfig()
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def run(self) -> None:
        """Execute the task."""
        raise NotImplementedError