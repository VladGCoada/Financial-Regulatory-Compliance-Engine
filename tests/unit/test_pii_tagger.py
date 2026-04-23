from frce.transformations.pii_tagger import PiiTagger, PII_TAG_MAP
from frce.config import FrceConfig


def test_pii_tag_map_covers_all_iban_columns():
    assert "debtor_iban" in PII_TAG_MAP
    assert "creditor_iban" in PII_TAG_MAP
    assert PII_TAG_MAP["debtor_iban"] == "class.iban"


def test_tagger_skips_gracefully_without_databricks_config():
    config = FrceConfig()
    tagger = PiiTagger(config=config)
    result = tagger.tag_table("frce_dev", "bronze", "raw_payments",
                               ["debtor_iban", "debtor_name"])
    assert result == {}


def test_is_databricks_false_without_token():
    config = FrceConfig()
    tagger = PiiTagger(config=config)
    assert tagger._is_databricks() is False