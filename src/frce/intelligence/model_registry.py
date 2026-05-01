from __future__ import annotations


class ModelRegistryClient:
    def register(self, model_name: str, run_id: str) -> str:
        return f"models:/{model_name}/staging?run_id={run_id}"
