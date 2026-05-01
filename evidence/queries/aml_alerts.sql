SELECT *
FROM gold.mart_aml_alerts
WHERE is_flagged = true
ORDER BY alert_created_at DESC;
