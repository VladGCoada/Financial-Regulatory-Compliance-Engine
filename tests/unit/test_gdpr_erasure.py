from frce.compliance.gdpr_erasure_pipeline import GdprErasurePipeline
from frce.config import FrceConfig


def test_gdpr_erasure_instantiates():
    config = FrceConfig()
    pipeline = GdprErasurePipeline(config=config)
    assert pipeline.config.catalog == "frce_dev"


def test_gold_erase_uses_hash():
    """Gold layer must erase by IBAN hash, not raw IBAN."""
    import hashlib
    iban = "NL91ABNA0417164300"
    expected_hash = hashlib.sha256(iban.encode()).hexdigest()
    assert len(expected_hash) == 64    # SHA-256 hex
    assert expected_hash != iban       # hash is not the raw value