from __future__ import annotations

from frce.config import FrceConfig
from frce.orchestration.pipeline_runner import run_batch_pipeline


def main() -> None:
    run_batch_pipeline(FrceConfig())


if __name__ == "__main__":
    main()
