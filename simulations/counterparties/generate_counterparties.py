from __future__ import annotations

import csv
import sys


def rows() -> list[dict[str, str]]:
    return [
        {
            "counterparty_id": "cp-001",
            "legal_name": "ACME BV",
            "country_code": "NL",
            "iban": "NL91ABNA0417164300",
            "email": "ops@example.com",
            "risk_tier": "LOW",
        }
    ]


if __name__ == "__main__":
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows()[0]))
    writer.writeheader()
    writer.writerows(rows())
