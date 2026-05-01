from frce.orchestration.dependency_graph import TASK_ORDER
from frce.orchestration.pipeline_runner import PipelineReport, TaskResult, run_batch_pipeline

__all__ = ["PipelineReport", "TASK_ORDER", "TaskResult", "run_batch_pipeline"]


def main() -> None:
    run_batch_pipeline()


if __name__ == "__main__":
    main()
