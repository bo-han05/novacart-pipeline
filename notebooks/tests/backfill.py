# Test: Backfill

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/ingestion/bronze_ingestion.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_orders.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_customers.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_products.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders.py").read())

def test_backfill():
    from pyspark.sql.functions import col
    
    # Verify orders from all dates are present, not just the most recent
    dates_present = [row["order_date"] for row in spark.table("silver_orders").select("order_date").distinct().collect()]
    dates_present = sorted(dates_present)
    print(f"Dates present in silver_orders: {dates_present}")
    
    expected_dates = ["2025-11-07", "2025-11-08", "2025-11-09", "2025-11-10"]
    
    for expected_date in expected_dates:
        assert str(expected_date) in [str(d) for d in dates_present], f"FAILED: Missing backfilled date {expected_date}"
    
    # Verify fact_orders correctly links to dim_date for all historical dates
    fact_dates = spark.table("fact_orders").join(
        spark.table("dim_date"), "date_sk"
    ).select("full_date").distinct().count()
    
    assert fact_dates == len(expected_dates), f"FAILED: Expected {len(expected_dates)} distinct dates in fact table, got {fact_dates}"
    
    print(f"Fact orders correctly spans {fact_dates} historical dates")
    print("TEST PASSED: Backfill - historical data correctly processed across all dates")

test_backfill()
