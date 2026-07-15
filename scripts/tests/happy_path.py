# Test: Happy Path
# Verifies clean pipeline run produces expected row counts

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/ingestion/bronze_ingestion
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_products
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_fact_orders

def test_happy_path():
    assert spark.table("bronze_orders").count() > 0, "Bronze orders should have data"
    assert spark.table("silver_orders").count() > 0, "Silver orders should have valid data"
    assert spark.table("fact_orders").count() > 0, "Fact orders should be populated"
    assert spark.table("dim_customer").filter("is_current = true").count() > 0, "Dim customer should have current records"
    assert spark.table("dim_product").count() > 0, "Dim product should have data"
    print("TEST PASSED: Happy path")

test_happy_path()
