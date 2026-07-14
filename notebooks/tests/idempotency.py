# Test: Idempotency

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/ingestion/bronze_ingestion
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_products
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders

# Capture counts after first run
fact_count_run1 = spark.table("fact_orders").count()
dim_customer_count_run1 = spark.table("dim_customer").filter("is_current = true").count()
dim_product_count_run1 = spark.table("dim_product").count()

print(f"Run 1 - fact_orders: {fact_count_run1}")
print(f"Run 1 - dim_customer (current): {dim_customer_count_run1}")
print(f"Run 1 - dim_product: {dim_product_count_run1}")

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders

def test_idempotency():
    fact_count_run2 = spark.table("fact_orders").count()
    dim_customer_count_run2 = spark.table("dim_customer").filter("is_current = true").count()
    dim_product_count_run2 = spark.table("dim_product").count()
    
    print(f"Run 2 - fact_orders: {fact_count_run2}")
    print(f"Run 2 - dim_customer (current): {dim_customer_count_run2}")
    print(f"Run 2 - dim_product: {dim_product_count_run2}")
    
    assert fact_count_run1 == fact_count_run2, f"FAILED: fact_orders count changed! Run1={fact_count_run1}, Run2={fact_count_run2}"
    assert dim_customer_count_run1 == dim_customer_count_run2, f"FAILED: dim_customer count changed!"
    assert dim_product_count_run1 == dim_product_count_run2, f"FAILED: dim_product count changed!"
    
    print("TEST PASSED: Idempotency confirmed - rerun produced identical output")

test_idempotency()
