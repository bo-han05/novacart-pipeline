# Test: Duplicate Handling

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/ingestion/bronze_ingestion
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_products
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders

def test_duplicate_handling():
    # Count occurrences of ORD-001 in bronze (should have duplicates from original data)
    bronze_dupe_count = spark.table("bronze_orders").filter("order_id = 'ORD-001'").count()
    
    # Count occurrences in silver (should be exactly 1 after dedup)
    silver_dupe_count = spark.table("silver_orders").filter("order_id = 'ORD-001'").count()
    
    # Count occurrences in fact_orders (should be exactly 1)
    fact_dupe_count = spark.table("fact_orders").filter("order_id = 'ORD-001'").count()
    
    print(f"Bronze ORD-001 count: {bronze_dupe_count}")
    print(f"Silver ORD-001 count: {silver_dupe_count}")
    print(f"Fact ORD-001 count: {fact_dupe_count}")
    
    assert bronze_dupe_count >= 2, "FAILED: Expected duplicates in Bronze (raw layer should preserve all)"
    assert silver_dupe_count == 1, f"FAILED: Silver should have exactly 1 row for ORD-001, got {silver_dupe_count}"
    assert fact_dupe_count == 1, f"FAILED: Fact table should have exactly 1 row for ORD-001, got {fact_dupe_count}"
    
    print("TEST PASSED: Duplicate handling")

test_duplicate_handling()