from frce.config import FrceConfig


def test_config_defaults() -> None:
    config = FrceConfig()

    assert config.app_name == "frce"
    assert config.environment == "dev"
    assert config.catalog == "frce_dev"
    assert config.bronze_raw_payments_table == "frce_dev.bronze.raw_payments"