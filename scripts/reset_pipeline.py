# Reset Pipeline - Clears all tables for a clean demo/test run
# WARNING: This deletes all data. Use only for testing/demo reset.

tables_to_clear = [
    "bronze_orders", "bronze_customers", "bronze_products",
    "silver_orders", "silver_customers", "silver_products",
    "quarantine_orders", "quarantine_customers", "quarantine_products",
    "quarantine_fact_orders",
    "dim_date", "dim_customer", "dim_product", "fact_orders",
    "pipeline_runs", "pipeline_run_steps",
    "pipeline_watermarks"
]

for t in tables_to_clear:
    spark.sql(f"DELETE FROM {t}")
    print(f"Cleared: {t}")

print("\nAll tables reset. Ready for fresh pipeline run.")
