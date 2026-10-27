# GH Archive Lakehouse

Incremental lakehouse on public GitHub event data (GH Archive): **AWS Lambda + S3** for ingestion,
**Databricks** (Auto Loader, Delta Lake MERGE, Workflows) for processing, **Terraform** for infra,
**GitHub Actions** for CI, and a Databricks dashboard on top.

```
GH Archive (hourly .json.gz)
        │  EventBridge (hourly) → Lambda
        ├──► S3 (raw archive copy)
        └──► Databricks UC Volume (landing)
                    │  Auto Loader (incremental, availableNow)
                    ▼
              BRONZE  raw JSON lines + lineage
                    │  parse → validate → dedupe → MERGE
                    ▼
              SILVER  typed events        QUARANTINE  bad rows + reason
                    │                     DQ_METRICS  per-batch counts
                    ▼
              GOLD    hourly activity · top repos/day · daily active actors
                    ▼
              AI/BI dashboard  (sql/dashboard_queries.sql)
```

## Dataset
[GH Archive](https://www.gharchive.org): every public GitHub event, one gzip file per hour,
`https://data.gharchive.org/YYYY-MM-DD-H.json.gz` (hour is **not** zero-padded, e.g. `2024-01-01-0`).
Each line is one JSON event (`id`, `type`, `actor`, `repo`, `payload`, `created_at`).
Check the size of an hourly file before you start; start with 6 to 24 hours, not weeks.

## Run order
1. **Unit tests (local):** `pip install -r requirements-dev.txt && pytest -q`
2. **Databricks:** import the repo as a Git folder. Run `notebooks/01_bronze_autoloader.py` once
   (it creates the schema and the volume).
3. **Land data (laptop first):**
   ```
   export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
   export DATABRICKS_TOKEN=<personal access token>
   export VOLUME_DIR=/Volumes/workspace/gh_lakehouse/raw/landing
   python ingestion/gharchive.py --start 2024-01-01T00 --hours 6
   ```
4. **Run the job:** notebooks 01 → 04 (or `databricks bundle deploy && databricks bundle run gh_lakehouse_pipeline`).
   Run it twice: the second run should add 0 rows (this proves incremental loading and idempotent MERGE).
5. **Automate ingestion:** `cd infra/terraform && terraform apply` (needs `raw_bucket_name`, `databricks_host`,
   `databricks_token`). EventBridge then triggers the Lambda every hour.
6. **Dashboard:** create an AI/BI dashboard from the queries in `sql/dashboard_queries.sql`.

## Design decisions
- **Raw JSON kept as text in bronze**, parsed in silver, so upstream schema changes never break ingestion.
- **MERGE on `event_id`** makes silver idempotent; duplicates across files are dropped before the merge.
- **Quarantine table** instead of failing the pipeline on bad rows; a **quality gate** task fails the job
  if the quarantine rate crosses a threshold.
- **Gold is a full recompute** (cheap at this size); silver and bronze are incremental.
- Databricks Free Edition is serverless-only with restricted outbound internet, so downloads happen in
  Lambda or on your laptop, not in a notebook.

## Fill these in after you run it (use real numbers only)
| Metric | Value |
|---|---|
| Hours of data / total compressed size | |
| Events processed (bronze rows) | |
| Quarantined rows (%) | |
| Duplicates dropped | |
| Time: full first run vs incremental re-run | |

## Known limits
- Free Edition is for non-commercial use and has daily compute quotas.
- Terraform, the bundle file and the notebooks were not run against a live account by the author of this
  scaffold; transforms and ingestion helpers are unit-tested.
- The Databricks token sits in SSM Parameter Store; for production use a service principal / Secrets Manager.
