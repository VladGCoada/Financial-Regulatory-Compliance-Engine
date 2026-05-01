"""Bronze ingestion adapters for FRCE."""

from frce.ingestion.counterparty_bronze import CounterpartyBronzeTask
from frce.ingestion.ecb_rates_bronze import ECBRatesBronzeTask, EcbRatesBronzeTask
from frce.ingestion.payments_stream_bronze import PaymentsStreamBronzeTask

__all__ = [
    "CounterpartyBronzeTask",
    "ECBRatesBronzeTask",
    "EcbRatesBronzeTask",
    "PaymentsStreamBronzeTask",
]
