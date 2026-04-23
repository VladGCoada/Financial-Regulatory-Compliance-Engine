from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from frce.base_task import BaseTask
from frce.compliance.dora_incident_classifier import classify_incident, Severity
from frce.config import FrceConfig

logger = logging.getLogger(__name__)


class MartDoraIncidentsTask(BaseTask):
    """
    Reads failed pipeline runs from audit.pipeline_runs,
    classifies each per DORA Article 17 EBA RTS,
    writes gold.mart_dora_incidents.
    """

    def __init__(self, config: FrceConfig) -> None:
        super().__init__(config)

    def read_failed_runs(self) -> DataFrame:
        return (
            self.get_spark()
            .read.format("delta")
            .table(f"{self.config.catalog}.audit.pipeline_runs")
            .filter(F.col("status") == "FAILED")
        )

    def classify_rows(self, df: DataFrame) -> list[dict]:
        incidents = []
        for row in df.collect():
            incident = classify_incident(
                run_id=row.run_id,
                task_name=row.task_name,
                affected_clients=0,
                transaction_value_eur=0.0,
                duration_minutes=0,
                is_cross_border=False,
            )
            incidents.append({
                "run_id": incident.run_id,
                "task_name": incident.task_name,
                "severity": incident.severity.value,
                "affected_clients_estimate": incident.affected_clients_estimate,
                "transaction_value_eur": incident.transaction_value_eur,
                "duration_minutes": incident.duration_minutes,
                "is_cross_border": incident.is_cross_border,
                "classification_rationale": incident.classification_rationale,
                "eba_report_required": incident.eba_report_required,
                "classified_at": row.completed_at,
            })
        return incidents

    def run(self) -> None:
        failed = self.read_failed_runs()
        incidents = self.classify_rows(failed)
        if not incidents:
            logger.info("No DORA incidents to write")
            return
        spark = self.get_spark()
        df = spark.createDataFrame(incidents)
        target = f"{self.config.catalog}.gold.mart_dora_incidents"
        df.write.format("delta").mode("append").saveAsTable(target)
        logger.info("MartDoraIncidentsTask: wrote %d incidents", len(incidents))