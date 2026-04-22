from pydantic_settings import BaseSettings, SettingsConfigDict


class FrceConfig(BaseSettings):
    app_name: str = "frce"
    environment: str = "dev"

    catalog: str = "frce_dev"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    audit_schema: str = "audit"
    compliance_schema: str = "compliance"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_payments_topic: str = "sepa-payments"

    ecb_base_url: str = "https://data-api.ecb.europa.eu"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )
    
    def table_name(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}"

    @property
    def bronze_raw_payments_table(self) -> str:
        return self.table_name(self.bronze_schema, "raw_payments")

    @property
    def bronze_counterparties_table(self) -> str:
        return self.table_name(self.bronze_schema, "counterparties")

    @property
    def bronze_fx_rates_table(self) -> str:
        return self.table_name(self.bronze_schema, "fx_rates")