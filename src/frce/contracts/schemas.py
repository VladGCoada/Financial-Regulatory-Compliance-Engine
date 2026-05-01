from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SEPA_PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("transaction_reference", StringType(), True),
        StructField("debtor_iban", StringType(), True),
        StructField("creditor_iban", StringType(), True),
        StructField("debtor_name", StringType(), True),
        StructField("creditor_name", StringType(), True),
        StructField("debtor_email", StringType(), True),
        StructField("creditor_email", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("booked_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("source_system", StringType(), True),
    ]
)

COUNTERPARTY_SCHEMA = StructType(
    [
        StructField("counterparty_id", StringType(), True),
        StructField("legal_name", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("iban", StringType(), True),
        StructField("email", StringType(), True),
        StructField("risk_tier", StringType(), True),
        StructField("onboarded_at", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("source_file", StringType(), True),
    ]
)

FX_RATE_SCHEMA = StructType(
    [
        StructField("rate_id", StringType(), True),
        StructField("base_currency", StringType(), True),
        StructField("target_currency", StringType(), True),
        StructField("rate", DoubleType(), True),
        StructField("rate_date", DateType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("source", StringType(), True),
    ]
)

MODEL_REGISTRY_SCHEMA = StructType(
    [
        StructField("inference_id", StringType(), False),
        StructField("model_name", StringType(), False),
        StructField("model_version", StringType(), False),
        StructField("mlflow_run_id", StringType(), False),
        StructField("training_data_version", StringType(), True),
        StructField("input_data_version", StringType(), True),
        StructField("scored_at", TimestampType(), True),
        StructField("input_row_count", DoubleType(), True),
        StructField("output_schema_hash", StringType(), True),
        StructField("explainability_sample", StringType(), True),
    ]
)
