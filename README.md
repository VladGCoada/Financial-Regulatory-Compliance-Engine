

 PySpark and Databricks-style folders to show how payment data can move
from raw files into cleaner tables, business reports, audit logs, and compliance
evidence.


The project follows a simple flow:

1. Load payment, counterparty, and exchange-rate data.
2. Clean and standardise the data.
3. Check the data for common quality problems.
4. Flag suspicious payment patterns for AML review.
5. Track GDPR erasure requests and DORA incident evidence.
6. Produce final tables that can be used for reporting and audits.



- data quality checks
- audit logs
- compliance evidence
- AML alert logic
- GDPR erasure tracking
- DORA incident classification
- Databricks job and bundle configuration


Use Python 3.11. PySpark 3.5 works best with this version.

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Run the tests:

```powershell
pytest -q
```

Expected result:

```text
44 passed
```


The project includes small command-line entry points:

```powershell
frce-batch
frce-streaming
frce-gdpr-erasure
frce-incident-classifier
```

Databricks workspace, catalog, storage account, secrets,
and job settings would need to be configured first FORREAL DEPLOYMENT

The Databricks bundle files are included in:

```text
databricks.yml
infra/bundle/
```

They describe example jobs for batch processing, streaming payments, GDPR
erasure handling, and model scoring.
