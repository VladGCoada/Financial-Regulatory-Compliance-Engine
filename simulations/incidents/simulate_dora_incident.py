from __future__ import annotations

from frce.compliance.dora.incident_classifier import classify_incident


if __name__ == "__main__":
    print(classify_incident("sim-run", "silver_payments", affected_clients=100000))
