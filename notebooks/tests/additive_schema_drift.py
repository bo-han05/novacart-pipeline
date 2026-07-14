# Test: Additive Schema Drift

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/ingestion/bronze_ingestion.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_orders.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_customers.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_products.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders.py").read())

def test_additive_schema_drift():
    # Confirm discount_code column exists (added earlier as schema drift test)
    columns = spark.table("bronze_orders").columns
    
    assert "discount_code" in columns, "FAILED: discount_code column should exist in bronze_orders"
    
    # Confirm pipeline didn't crash - bronze_orders should have rows
    row_count = spark.table("bronze_orders").count()
    assert row_count > 0, "FAILED: bronze_orders should have data despite schema drift"
    
    print(f"Bronze orders columns: {columns}")
    print(f"Bronze orders row count: {row_count}")
    print("TEST PASSED: Additive schema drift handled correctly (new column tolerated, pipeline continued)")

test_additive_schema_drift()