from __future__ import annotations

from frce.gold.dim_counterparty import DimCounterpartyTask
from frce.gold.fact_payments import FactPaymentsTask
from frce.gold.mart_aml_alerts import MartAmlAlertsTask
from frce.gold.mart_dora_incidents import MartDoraIncidentsTask
from frce.gold.mart_gdpr_requests import MartGdprRequestsTask
from frce.transformations.silver_counterparty import SilverCounterpartyTask
from frce.transformations.silver_fx import SilverFxTask
from frce.transformations.silver_payments import SilverPaymentsTask

TASK_CLASSES = {
    "silver_payments": SilverPaymentsTask,
    "silver_counterparty": SilverCounterpartyTask,
    "silver_fx": SilverFxTask,
    "fact_payments": FactPaymentsTask,
    "dim_counterparty": DimCounterpartyTask,
    "mart_aml_alerts": MartAmlAlertsTask,
    "mart_dora_incidents": MartDoraIncidentsTask,
    "mart_gdpr_requests": MartGdprRequestsTask,
}

ALL_TASK_NAMES = list(TASK_CLASSES)
