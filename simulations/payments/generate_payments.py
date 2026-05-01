from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timezone


def fake_payment() -> dict:
    return {
        "payment_id": str(uuid.uuid4()),
        "transaction_reference": f"TXN-{random.randint(100000, 999999)}",
        "debtor_iban": "NL91ABNA0417164300",
        "creditor_iban": "DE89370400440532013000",
        "amount": round(random.uniform(10, 150000), 2),
        "currency": "EUR",
        "country_code": random.choice(["NL", "DE", "FR", "IR"]),
        "booked_at": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    print(json.dumps(fake_payment()))
