# novacart-pipeline

A re-runnable ETL pipeline built on Databricks and Delta Lake, implementing a Bronze/Silver/Gold lakehouse architecture with SCD1/SCD2 dimensions, schema drift handling, and full observability.

### Architecture
- `fact_orders` — transactions
- `dim_customer` — SCD2 (history preserved)
- `dim_product` — SCD1 (overwrite)
- `dim_date` — calendar

### Setup
1. Clone into Databricks Repos
2. Ensure source data exists in `data/landing/` within the repo:
   - `customers.json` — nested customer records
   - `orders/*.csv` — daily order files
   - `products.csv` — extracted from source SQLite database (`products.db`)
3. Update `BASE_PATH` in `notebooks/ingestion/bronze_ingestion.py`

### Run
**Single command:** Workflows → Jobs → **novacart-pipeline** → Run Now

The Job executes the full pipeline in order:
- `bronze_ingestion`
- `silver_customers`
- `silver_orders`
- `silver_products`
- `gold_dim_date`
- `gold_dim_customer`
- `gold_dim_product`
- `gold_fact_orders`

Reset for demo: run `notebooks/reset_pipeline.py`

### Tests
Run each notebook in `notebooks/tests/` individually — all print `TEST PASSED`:
happy path, duplicates, quarantine, additive/subtractive schema drift, idempotency, backfill

### Observability
Query `pipeline_runs` and `pipeline_run_steps` for status, duration, row counts, and errors.

### Data Quality
Bad rows are quarantined (never dropped) in `quarantine_orders/customers/products/fact_orders`, each with a reason.

### Schema Drift
- New column → warning, pipeline continues
- Missing required column → exception, pipeline stops

### Idempotency & Incremental Loading
Reruns produce identical output. Products load incrementally via `updated_at` watermark.
