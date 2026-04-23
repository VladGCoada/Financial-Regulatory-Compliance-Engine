from __future__ import annotations

import argparse
import json
import random
import uuid
from pathlib import Path

from faker import Faker

fake = Faker(["nl_NL", "de_DE", "da_DK"])
CURRENCIES = ["EUR", "USD", "GBP", "CHF", "SEK", "DKK"]
SOURCE_SYSTEMS = ["SWIFT_GPI", "SEPA_CT", "SEPA_INST", "TARGET2"]
BAD_RECORD_RATE = 0.03


def _make_good() -> dict:
    return {
        "payment_id": str(uuid.uuid4()),
        "transaction_reference": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "debtor_iban": fake.iban(),
        "creditor_iban": fake.iban(),
        "debtor_name": fake.company(),
        "creditor_name": fake.company(),
        "debtor_email": fake.company_email(),
        "creditor_email": fake.company_email(),
        "amount": round(random.uniform(10.0, 500000.0), 2),
        "currency": random.choice(CURRENCIES),
        "country_code": random.choice(["NL", "DE", "DK", "BE", "FR"]),
        "booked_at": fake.iso8601(),
        "source_system": random.choice(SOURCE_SYSTEMS),
        "_is_bad": False,
    }


def _corrupt(record: dict) -> dict:
    fault = random.choice(["null_iban", "negative_amount", "bad_currency", "null_id"])
    if fault == "null_iban":
        record["debtor_iban"] = None
    elif fault == "negative_amount":
        record["amount"] = -abs(record["amount"])
    elif fault == "bad_currency":
        record["currency"] = "XYZ"
    elif fault == "null_id":
        record["payment_id"] = None
    record["_is_bad"] = True
    return record


def generate(rows: int, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    bad_count = 0

    with out.open("w", encoding="utf-8") as fh:
        for _ in range(rows):
            record = _make_good()
            if random.random() < BAD_RECORD_RATE:
                record = _corrupt(record)
                bad_count += 1
            fh.write(json.dumps(record) + "\n")

    print(f"Written {rows} records ({bad_count} bad) to {out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--out", type=Path, default=Path("data/payments.jsonl"))
    args = parser.parse_args()
    generate(args.rows, args.out)