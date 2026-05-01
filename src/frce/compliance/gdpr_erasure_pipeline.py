from frce.compliance.gdpr.erasure_pipeline import GDPRErasurePipeline, GdprErasurePipeline

__all__ = ["GDPRErasurePipeline", "GdprErasurePipeline"]


def main() -> None:
    from frce.config import FrceConfig

    GdprErasurePipeline(FrceConfig()).run()
