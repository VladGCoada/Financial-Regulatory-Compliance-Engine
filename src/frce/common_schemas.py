from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
    DoubleType,
)


SEPA_PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("transaction_reference", StringType(), False),
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

COUNTERPARTY_SCHEMA = StructType([
    StructField("counterparty_id", StringType(), False),
    StructField("legal_name", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("iban", StringType(), True),
    StructField("email", StringType(), True),
    StructField("risk_tier", StringType(), True),        # LOW / MEDIUM / HIGH
    StructField("onboarded_at", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("source_file", StringType(), True),
])

FX_RATE_SCHEMA = StructType([
    StructField("rate_id", StringType(), False),
    StructField("base_currency", StringType(), False),   # always EUR
    StructField("target_currency", StringType(), False),
    StructField("rate", DoubleType(), False),
    StructField("rate_date", TimestampType(), False),
    StructField("ingested_at", TimestampType(), True),
    StructField("source", StringType(), True),           # ECB
])