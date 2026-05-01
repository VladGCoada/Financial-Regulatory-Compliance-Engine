"""Gold evidence tables and marts for FRCE."""

from frce.gold.dim_counterparty import DimCounterpartyTask
from frce.gold.fact_payments import FactPaymentsTask
from frce.gold.mart_aml_alerts import MartAmlAlertsTask
from frce.gold.mart_dora_incidents import MartDoraIncidentsTask
from frce.gold.mart_gdpr_requests import MartGdprRequestsTask
from frce.gold.mart_model_registry import MartModelRegistryTask

__all__ = [
    "DimCounterpartyTask",
    "FactPaymentsTask",
    "MartAmlAlertsTask",
    "MartDoraIncidentsTask",
    "MartGdprRequestsTask",
    "MartModelRegistryTask",
]
