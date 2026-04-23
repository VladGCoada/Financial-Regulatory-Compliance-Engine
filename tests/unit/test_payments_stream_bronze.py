from frce.ingestion.payments_stream_bronze import PaymentsStreamBronzeTask
from frce.config import FrceConfig


def test_payments_stream_bronze_instantiates():
    config = FrceConfig()
    task = PaymentsStreamBronzeTask(config=config)
    assert task.config.payments_bronze_table.startswith("frce_dev")