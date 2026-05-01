from __future__ import annotations

import logging
from dataclasses import dataclass

from frce.contracts.counterparty_contract import validate_counterparty_columns
from frce.contracts.evidence_contract import missing_model_evidence_fields
from frce.contracts.fx_contract import validate_fx_columns
from frce.contracts.payment_contract import validate_payment_columns

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ContractValidationResult:
    contract_name: str
    passed: bool
    missing_columns: list[str]
    present_columns: list[str]
    message: str


class ContractValidator:
    def validate_payments(self, columns: list[str]) -> ContractValidationResult:
        missing = validate_payment_columns(columns)
        return self._result("payments", columns, missing)

    def validate_counterparty(self, columns: list[str]) -> ContractValidationResult:
        missing = validate_counterparty_columns(columns)
        return self._result("counterparty", columns, missing)

    def validate_fx(self, columns: list[str]) -> ContractValidationResult:
        missing = validate_fx_columns(columns)
        return self._result("fx_rates", columns, missing)

    def validate_model_evidence(self, columns: list[str]) -> ContractValidationResult:
        missing = missing_model_evidence_fields(columns)
        return self._result("model_evidence", columns, missing)

    def validate_all(self, table_columns: dict[str, list[str]]) -> list[ContractValidationResult]:
        dispatch = {
            "payments": self.validate_payments,
            "counterparty": self.validate_counterparty,
            "fx": self.validate_fx,
            "fx_rates": self.validate_fx,
            "model_evidence": self.validate_model_evidence,
        }
        results: list[ContractValidationResult] = []
        for name, columns in table_columns.items():
            validator = dispatch.get(name)
            if validator is None:
                logger.warning("No contract validator found for %s", name)
                continue
            results.append(validator(columns))
        return results

    @property
    def all_contracts(self) -> list[str]:
        return ["payments", "counterparty", "fx_rates", "model_evidence"]

    @staticmethod
    def _result(
        contract_name: str,
        columns: list[str],
        missing: list[str],
    ) -> ContractValidationResult:
        return ContractValidationResult(
            contract_name=contract_name,
            passed=not missing,
            missing_columns=missing,
            present_columns=[column for column in columns if column not in missing],
            message="OK" if not missing else f"Missing: {missing}",
        )
