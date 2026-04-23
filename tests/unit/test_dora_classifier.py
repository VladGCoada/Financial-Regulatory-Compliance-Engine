from frce.compliance.dora_incident_classifier import classify_incident, Severity


def test_major_by_client_count():
    incident = classify_incident("run-1", "payments_task", affected_clients=150_000)
    assert incident.severity == Severity.MAJOR
    assert incident.eba_report_required is True


def test_major_by_duration():
    incident = classify_incident("run-2", "payments_task", duration_minutes=180)
    assert incident.severity == Severity.MAJOR


def test_significant_by_value():
    incident = classify_incident("run-3", "payments_task", transaction_value_eur=750_000)
    assert incident.severity == Severity.SIGNIFICANT


def test_minor_below_thresholds():
    incident = classify_incident("run-4", "payments_task",
                                  affected_clients=100, duration_minutes=5)
    assert incident.severity == Severity.MINOR
    assert incident.eba_report_required is False


def test_cross_border_significant_requires_report():
    incident = classify_incident("run-5", "payments_task",
                                  affected_clients=15_000, is_cross_border=True)
    assert incident.eba_report_required is True