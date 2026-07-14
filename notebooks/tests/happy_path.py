# Test: Happy Path
# Verifies clean pipeline run produces expected row counts

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/ingestion/bronze_ingestion.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_orders.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_customers.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_products.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer.py").read())
exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders.py").read())

def test_happy_path():
    assert spark.table("bronze_orders").count() > 0, "Bronze orders should have data"
    assert spark.table("silver_orders").count() > 0, "Silver orders should have valid data"
    assert spark.table("fact_orders").count() > 0, "Fact orders should be populated"
    assert spark.table("dim_customer").filter("is_current = true").count() > 0, "Dim customer should have current records"
    assert spark.table("dim_product").count() > 0, "Dim product should have data"
    print("TEST PASSED: Happy path")

test_happy_path()
