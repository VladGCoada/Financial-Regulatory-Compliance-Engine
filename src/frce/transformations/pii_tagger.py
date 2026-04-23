from __future__ import annotations

import logging

import requests

from frce.config import FrceConfig

logger = logging.getLogger(__name__)

PII_TAG_MAP = {
    "debtor_iban": "class.iban",
    "creditor_iban": "class.iban",
    "debtor_name": "class.name",
    "creditor_name": "class.name",
    "debtor_email": "class.email_address",
    "creditor_email": "class.email_address",
    "iban": "class.iban",
    "email": "class.email_address",
    "legal_name": "class.name",
}


class PiiTagger:
    """
    Applies Unity Catalog column tags to PII columns.
    Tags trigger ABAC masking policies defined in abac_policies.tf.
    No-op in local mode (no UC endpoint available).
    """

    def __init__(self, config: FrceConfig) -> None:
        self.config = config
        self.base_url = getattr(config, "databricks_host", None)
        self.token = getattr(config, "databricks_token", None)

    def _is_databricks(self) -> bool:
        return bool(self.base_url and self.token)

    def tag_table(self, catalog: str, schema: str, table: str,
                  columns: list[str]) -> dict[str, str]:
        """
        Tags PII columns. Returns dict of {column: tag_applied}.
        Skips gracefully if not running on Databricks.
        """
        applied = {}
        if not self._is_databricks():
            logger.info(
                "PiiTagger: no Databricks endpoint configured — skipping UC tagging"
            )
            return applied

        for col in columns:
            tag = PII_TAG_MAP.get(col)
            if not tag:
                continue
            url = (
                f"{self.base_url}/api/2.1/unity-catalog/tables/"
                f"{catalog}.{schema}.{table}/columns/{col}/tags"
            )
            try:
                resp = requests.put(
                    url,
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={"tags": [{"tag_name": tag}]},
                    timeout=10,
                )
                resp.raise_for_status()
                applied[col] = tag
                logger.info("Tagged %s.%s.%s.%s as %s", catalog, schema, table, col, tag)
            except Exception as exc:
                logger.warning("Failed to tag %s: %s", col, exc)
        return applied