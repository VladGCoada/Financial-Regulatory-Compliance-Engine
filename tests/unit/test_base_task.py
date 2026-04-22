from frce.base_task import BaseTask
from frce.config import FrceConfig


class DummyTask(BaseTask):
    def run(self) -> None:
        return None


def test_base_task_uses_config() -> None:
    config = FrceConfig(catalog="frce_test")
    task = DummyTask(config=config)

    assert task.config.catalog == "frce_test"
    assert task.logger is not None