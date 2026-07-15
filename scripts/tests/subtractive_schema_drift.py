# Test: Subtractive Schema Drift

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/ingestion/bronze_ingestion
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_products
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_fact_orders

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/schema_definitions
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/schema_validator

BASE_PATH = "/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/data/landing"

def test_subtractive_schema_drift():
    from pyspark.sql import Row
    import tempfile
    
    # Create a bad CSV file missing required column 'unit_price'
    bad_csv_content = "order_id,customer_id,product_id,order_date,quantity,status\nORD-999,CUST-001,PROD-001,2025-11-20,1,shipped\n"
    
    bad_file_path = f"{BASE_PATH}/orders/orders_test_bad_schema.csv"
    dbutils.fs.put(bad_file_path, bad_csv_content, overwrite=True)
    
    exception_raised = False
    error_message = ""
    
    try:
        file_list = [f.path for f in dbutils.fs.ls(f"{BASE_PATH}/orders") if f.name.startswith("orders_") and f.name.endswith(".csv")]
        for f in file_list:
            df = spark.read.option("header", True).option("inferSchema", True).csv(f)
            validate_schema(df, ORDERS_REQUIRED, f"orders ({f})")
    except Exception as e:
        exception_raised = True
        error_message = str(e)
    
    # Cleanup test file regardless of outcome
    dbutils.fs.rm(bad_file_path)
    
    assert exception_raised, "FAILED: Pipeline should have raised an exception for missing required column"
    assert "unit_price" in error_message, f"FAILED: Error message should mention missing column. Got: {error_message}"
    
    print(f"Exception correctly raised: {error_message}")
    print("TEST PASSED: Subtractive schema drift correctly caught, pipeline failed fast")

test_subtractive_schema_drift()
