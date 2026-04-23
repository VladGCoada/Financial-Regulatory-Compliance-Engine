from frce.intelligence.anomaly_detector import AnomalyDetector, FEATURE_COLS
from frce.config import FrceConfig


def test_feature_cols_cover_dora_requirement():
    assert "amount" in FEATURE_COLS
    assert "is_cross_border_int" in FEATURE_COLS


def test_engineer_features_adds_expected_columns(spark):
    from datetime import datetime, timezone
    config = FrceConfig()
    detector = AnomalyDetector(config=config)
    detector._spark = spark

    data = [("p-001", 500.0, "NL", datetime(2024, 1, 15, 14, 30, 0))]
    df = spark.createDataFrame(data, ["payment_id", "amount", "country_code", "booked_at"])

    result = detector.engineer_features(df)
    cols = result.columns
    assert "hour_of_day" in cols
    assert "day_of_week" in cols
    assert "is_cross_border_int" in cols


def test_anomaly_detector_instantiates():
    config = FrceConfig()
    detector = AnomalyDetector(config=config)
    assert detector.config.catalog == "frce_dev"