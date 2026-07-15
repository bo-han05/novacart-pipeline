# Test: Additive Schema Drift

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/ingestion/bronze_ingestion
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_products
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_fact_orders

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