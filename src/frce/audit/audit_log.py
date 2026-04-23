from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


class AuditLog:
    """
    Writes pipeline run records to audit.pipeline_runs.
    Statuses: STARTED, COMPLETED, FAILED.
    FAILED rows are evaluated by dora_incident_classifier.
    """

    STATUS_STARTED = "STARTED"
    STATUS_COMPLETED = "COMPLETED"
    STATUS_FAILED = "FAILED"

    def __init__(self, spark: Any, catalog: str) -> None:
        self.spark = spark
        self.table = f"{catalog}.audit.pipeline_runs"

    def _write(self, record: dict) -> None:
        df = self.spark.createDataFrame([record])
        df.write.format("delta").mode("append").saveAsTable(self.table)

    def start(self, task_name: str, run_metadata: dict | None = None) -> str:
        run_id = str(uuid.uuid4())
        self._write({
            "run_id": run_id,
            "task_name": task_name,
            "status": self.STATUS_STARTED,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": None,
            "error_message": None,
            "metadata": str(run_metadata or {}),
        })
        return run_id

    def complete(self, run_id: str, task_name: str) -> None:
        self._write({
            "run_id": run_id,
            "task_name": task_name,
            "status": self.STATUS_COMPLETED,
            "started_at": None,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": None,
            "metadata": "{}",
        })

    def fail(self, run_id: str, task_name: str, error: str) -> None:
        self._write({
            "run_id": run_id,
            "task_name": task_name,
            "status": self.STATUS_FAILED,
            "started_at": None,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": error[:2000],
            "metadata": "{}",
        })
        logger.error("Pipeline FAILED — run_id=%s task=%s: %s", run_id, task_name, error)