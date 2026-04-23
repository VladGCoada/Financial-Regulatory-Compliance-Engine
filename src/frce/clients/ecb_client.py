from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

ECB_BASE = "https://data-api.ecb.europa.eu/service/data/EXR"


class EcbClient:
    """Fetches EUR FX rates from the ECB SDMX-JSON API."""

    def __init__(self, timeout: int = 10) -> None:
        self.timeout = timeout

    def get_rates(self, currencies: list[str], ref_date: date | None = None) -> list[dict[str, Any]]:
        results = []
        for ccy in currencies:
            url = f"{ECB_BASE}/D.{ccy}.EUR.SP00.A"
            params: dict[str, str] = {"format": "jsondata", "detail": "dataonly"}
            if ref_date:
                params["startPeriod"] = ref_date.isoformat()
                params["endPeriod"] = ref_date.isoformat()
            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
                observations = (
                    data.get("dataSets", [{}])[0]
                    .get("series", {})
                    .get("0:0:0:0:0", {})
                    .get("observations", {})
                )
                periods = (
                    data.get("structure", {})
                    .get("dimensions", {})
                    .get("observation", [{}])[0]
                    .get("values", [])
                )
                for idx, period_meta in enumerate(periods):
                    obs = observations.get(str(idx))
                    if obs:
                        results.append({
                            "base_currency": "EUR",
                            "target_currency": ccy,
                            "rate": float(obs[0]),
                            "rate_date": period_meta.get("id"),
                        })
            except Exception as exc:
                logger.warning("ECB fetch failed for %s: %s", ccy, exc)
        return results