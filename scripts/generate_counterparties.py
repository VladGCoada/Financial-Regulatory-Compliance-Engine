from __future__ import annotations

import argparse
import csv
import random
import uuid
from pathlib import Path

from faker import Faker

LOCALES = ["nl_NL", "de_DE", "da_DK"]
RISK_TIERS = ["LOW", "LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH"]


def generate(rows: int, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    fakers = {loc: Faker(loc) for loc in LOCALES}

    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "counterparty_id",
                "legal_name",
                "country_code",
                "iban",
                "email",
                "risk_tier",
                "onboarded_at",
            ],
        )
        writer.writeheader()

        for _ in range(rows):
            locale = random.choice(LOCALES)
            fake = fakers[locale]
            country = locale.split("_")[1]
            writer.writerow(
                {
                    "counterparty_id": str(uuid.uuid4()),
                    "legal_name": fake.company(),
                    "country_code": country,
                    "iban": fake.iban(),
                    "email": fake.company_email(),
                    "risk_tier": random.choice(RISK_TIERS),
                    "onboarded_at": fake.iso8601(),
                }
            )

    print(f"Written {rows} rows to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--out", type=Path, default=Path("data/counterparties.csv"))
    args = parser.parse_args()
    generate(args.rows, args.out)