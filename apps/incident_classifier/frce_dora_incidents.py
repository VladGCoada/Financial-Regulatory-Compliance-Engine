from __future__ import annotations

from frce.config import FrceConfig
from frce.gold.mart_dora_incidents import MartDoraIncidentsTask


def main() -> None:
    MartDoraIncidentsTask(FrceConfig()).run()


if __name__ == "__main__":
    main()
