from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from frce.audit.audit_log import AuditLog
from frce.config import FrceConfig
from frce.orchestration.dependency_graph import TASK_ORDER
from frce.orchestration.task_registry import TASK_CLASSES
from frce.utils.logging import configure_logging
from frce.utils.spark import get_spark

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskResult:
    task_name: str
    run_id: str
    success: bool
    duration_seconds: float
    error: str | None = None


@dataclass
class PipelineReport:
    results: list[TaskResult] = field(default_factory=list)

    @property
    def succeeded(self) -> list[TaskResult]:
        return [result for result in self.results if result.success]

    @property
    def failed(self) -> list[TaskResult]:
        return [result for result in self.results if not result.success]

    @property
    def all_passed(self) -> bool:
        return not self.failed

    @property
    def total_seconds(self) -> float:
        return sum(result.duration_seconds for result in self.results)

    def summary(self) -> str:
        return (
            f"Pipeline: {len(self.succeeded)} passed / "
            f"{len(self.failed)} failed / {self.total_seconds:.1f}s total"
        )


def run_batch_pipeline(
    config: FrceConfig | None = None,
    halt_on_first_failure: bool = True,
) -> PipelineReport:
    config = config or FrceConfig()
    configure_logging()
    spark = get_spark("frce-batch")
    audit = AuditLog(spark, config.catalog)
    report = PipelineReport()

    for task_name in TASK_ORDER:
        task_cls = TASK_CLASSES[task_name]
        run_id = audit.start(task_name)
        started = time.monotonic()
        try:
            task_cls(config).run()
            duration = time.monotonic() - started
            audit.complete(run_id, task_name)
            report.results.append(TaskResult(task_name, run_id, True, duration))
            logger.info("completed %s in %.1fs", task_name, duration)
        except Exception as exc:
            duration = time.monotonic() - started
            error = f"{type(exc).__name__}: {exc}"
            audit.fail(run_id, task_name, error)
            report.results.append(TaskResult(task_name, run_id, False, duration, error))
            logger.error("failed %s in %.1fs: %s", task_name, duration, error)
            if halt_on_first_failure:
                raise

    logger.info(report.summary())
    return report
