from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ErasureAuditRecord:
    request_id: str
    bronze_rows_deleted: int
    silver_rows_deleted: int
    gold_rows_deleted: int
    operator: str = "gdpr_erasure_pipeline"
