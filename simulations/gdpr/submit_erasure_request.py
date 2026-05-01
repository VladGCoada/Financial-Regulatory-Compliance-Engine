from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone


def request(entity_id: str) -> dict:
    return {
        "request_id": str(uuid.uuid4()),
        "entity_type": "DEBTOR_IBAN",
        "entity_id": entity_id,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "status": "PENDING",
    }


if __name__ == "__main__":
    print(json.dumps(request("NL91ABNA0417164300")))
