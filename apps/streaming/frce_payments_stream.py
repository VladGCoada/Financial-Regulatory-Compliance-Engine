from __future__ import annotations

from frce.config import FrceConfig
from frce.ingestion.payments_stream_bronze import PaymentsStreamBronzeTask


def main() -> None:
    PaymentsStreamBronzeTask(FrceConfig()).run()


if __name__ == "__main__":
    main()
