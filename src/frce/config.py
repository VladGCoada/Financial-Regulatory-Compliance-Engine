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

    storage_account: str = "frceplaceholder"

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
    def counterparty_bronze_table(self) -> str:
        return self.table_name(self.bronze_schema, "counterparties")

    @property
    def fx_rates_bronze_table(self) -> str:
        return self.table_name(self.bronze_schema, "fx_rates")

    @property
    def quarantine_payments_table(self) -> str:
        return self.table_name(self.bronze_schema, "quarantine_payments")

    @property
    def payments_bronze_table(self) -> str:
        return self.bronze_raw_payments_table

    @property
    def silver_payments_table(self) -> str:
        return self.table_name(self.silver_schema, "payments_clean")

    @property
    def silver_counterparty_table(self) -> str:
        return self.table_name(self.silver_schema, "counterparty")

    @property
    def silver_fx_rates_table(self) -> str:
        return self.table_name(self.silver_schema, "fx_rates")

    @property
    def audit_pipeline_runs_table(self) -> str:
        return self.table_name(self.audit_schema, "pipeline_runs")

    @property
    def audit_erasure_audit_table(self) -> str:
        return self.table_name(self.audit_schema, "erasure_audit")

    @property
    def compliance_erasure_requests_table(self) -> str:
        return self.table_name(self.compliance_schema, "erasure_requests")

    @property
    def payments_landing_path(self) -> str:
        return f"abfss://landing@{self.storage_account}.dfs.core.windows.net/payments/"

    @property
    def payments_schema_location(self) -> str:
        return f"abfss://bronze@{self.storage_account}.dfs.core.windows.net/_schema/payments/"

    @property
    def payments_checkpoint_path(self) -> str:
        return f"abfss://bronze@{self.storage_account}.dfs.core.windows.net/_checkpoint/payments/"

    @property
    def counterparty_source_path(self) -> str:
        return f"abfss://bronze@{self.storage_account}.dfs.core.windows.net/counterparties/"