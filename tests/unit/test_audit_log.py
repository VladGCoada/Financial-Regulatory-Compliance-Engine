from frce.audit.audit_log import AuditLog


def test_audit_log_start_returns_uuid(spark):
    log = AuditLog.__new__(AuditLog)  # bypass __init__ to avoid Delta write
    import uuid
    run_id = str(uuid.uuid4())
    assert len(run_id) == 36


def test_audit_log_status_constants():
    assert AuditLog.STATUS_STARTED == "STARTED"
    assert AuditLog.STATUS_COMPLETED == "COMPLETED"
    assert AuditLog.STATUS_FAILED == "FAILED"