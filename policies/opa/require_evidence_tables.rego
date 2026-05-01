package frce.evidence

deny[msg] {
  required := {"gold.mart_aml_alerts", "gold.mart_dora_incidents", "gold.mart_gdpr_requests", "gold.mart_model_registry"}[_]
  not input.tables[required]
  msg := sprintf("missing evidence table %s", [required])
}
