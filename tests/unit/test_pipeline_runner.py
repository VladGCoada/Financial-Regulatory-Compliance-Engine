from frce.pipeline_runner import TASK_ORDER


def test_task_order_has_correct_dependencies():
    assert TASK_ORDER.index("silver_payments") < TASK_ORDER.index("fact_payments")
    assert TASK_ORDER.index("fact_payments") < TASK_ORDER.index("mart_aml_alerts")
    assert TASK_ORDER.index("mart_dora_incidents") > TASK_ORDER.index("silver_payments")


def test_all_gold_tasks_are_after_silver():
    gold_tasks = [t for t in TASK_ORDER if t.startswith(("fact_", "mart_", "dim_"))]
    silver_tasks = [t for t in TASK_ORDER if t.startswith("silver_")]
    last_silver = max(TASK_ORDER.index(t) for t in silver_tasks)
    for gold in gold_tasks:
        assert TASK_ORDER.index(gold) > last_silver