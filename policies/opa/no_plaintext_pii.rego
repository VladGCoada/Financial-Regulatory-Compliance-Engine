package frce.pii

deny[msg] {
  input.layer == "gold"
  pii := {"debtor_iban", "creditor_iban", "debtor_email", "creditor_email"}[_]
  input.columns[_] == pii
  msg := sprintf("plaintext PII column %s is not allowed in gold", [pii])
}
