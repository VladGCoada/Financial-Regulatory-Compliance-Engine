from __future__ import annotations

from dataclasses import dataclass

from frce.config.settings import FrceConfig


@dataclass(frozen=True)
class TableSpec:
    name: str
    layer: str
    pii: bool
    owner: str
    regulatory_purpose: str


def build_table_registry(config: FrceConfig) -> dict[str, TableSpec]:
    return {
        "bronze.raw_payments": TableSpec(
            config.payments_bronze_table,
            "bronze",
            True,
            "payments_platform",
            "Raw SEPA payment evidence",
        ),
        "bronze.counterparties": TableSpec(
            config.counterparty_bronze_table,
            "bronze",
            True,
            "reference_data",
            "Raw counterparty master input",
        ),
        "bronze.fx_rates": TableSpec(
            config.fx_rates_bronze_table,
            "bronze",
            False,
            "finance_data",
            "Raw ECB exchange-rate evidence",
        ),
        "silver.payments_clean": TableSpec(
            config.silver_payments_table,
            "silver",
            True,
            "payments_platform",
            "DQ-validated payment records",
        ),
        "silver.counterparties": TableSpec(
            config.silver_counterparty_table,
            "silver",
            True,
            "reference_data",
            "SCD2 counterparty master",
        ),
        "silver.fx_rates": TableSpec(
            config.silver_fx_rates_table,
            "silver",
            False,
            "finance_data",
            "Cleaned ECB FX rates",
        ),
        "gold.fact_payments": TableSpec(
            config.gold_fact_payments_table,
            "gold",
            False,
            "payments_platform",
            "Hashed analytical payment fact",
        ),
        "gold.dim_counterparty": TableSpec(
            config.gold_dim_counterparty_table,
            "gold",
            False,
            "reference_data",
            "Current counterparty dimension",
        ),
        "gold.mart_aml_alerts": TableSpec(
            config.gold_mart_aml_alerts_table,
            "gold",
            False,
            "compliance_aml",
            "DORA Article 9 AML/anomaly evidence",
        ),
        "gold.mart_dora_incidents": TableSpec(
            config.gold_mart_dora_incidents_table,
            "gold",
            False,
            "operational_resilience",
            "DORA Article 17 incident register",
        ),
        "gold.mart_gdpr_requests": TableSpec(
            config.gold_mart_gdpr_requests_table,
            "gold",
            False,
            "privacy_office",
            "GDPR Article 17 erasure evidence",
        ),
        "gold.mart_model_registry": TableSpec(
            config.gold_mart_model_registry_table,
            "gold",
            False,
            "model_risk",
            "EU AI Act Article 13 inference transparency evidence",
        ),
    }
