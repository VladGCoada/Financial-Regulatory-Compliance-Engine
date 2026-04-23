from frce.transformations.silver_counterparty import SilverCounterpartyTask
from frce.config import FrceConfig


def test_scd2_change_condition_covers_all_tracked_cols():
    config = FrceConfig()
    task = SilverCounterpartyTask(config=config)
    condition = task._change_condition()
    for col in task.SCD2_COLS:
        assert col in condition


def test_scd2_task_instantiates():
    config = FrceConfig()
    task = SilverCounterpartyTask(config=config)
    assert task.config.catalog == "frce_dev"
    assert len(task.SCD2_COLS) >= 3