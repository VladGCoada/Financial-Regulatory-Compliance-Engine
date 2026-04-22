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