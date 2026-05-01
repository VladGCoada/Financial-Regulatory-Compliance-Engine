from __future__ import annotations

from frce.config import FrceConfig
from frce.intelligence.anomaly_detector import AnomalyDetector


def main() -> None:
    AnomalyDetector(FrceConfig()).run()


if __name__ == "__main__":
    main()
