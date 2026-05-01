from __future__ import annotations

from frce.compliance.gdpr.erasure_pipeline import GdprErasurePipeline
from frce.config import FrceConfig


def main() -> None:
    GdprErasurePipeline(FrceConfig()).run()


if __name__ == "__main__":
    main()
