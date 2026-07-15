# Test: Bad Data Quarantined

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/ingestion/bronze_ingestion
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/transform/silver_products
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/gold/gold_fact_orders

def test_quarantine():
    # ORD-004 has quantity=0, should be quarantined
    quarantined_order = spark.table("quarantine_orders").filter("order_id = 'ORD-004'").count()
    # CUST-005 has invalid email, should be quarantined
    quarantined_customer = spark.table("quarantine_customers").filter("customer_id = 'CUST-005'").count()
    # Valid orders should still process successfully alongside bad ones
    valid_orders_count = spark.table("silver_orders").count()
    
    print(f"Quarantined ORD-004 count: {quarantined_order}")
    print(f"Quarantined CUST-005 count: {quarantined_customer}")
    print(f"Valid silver orders count: {valid_orders_count}")
    
    assert quarantined_order == 1, f"FAILED: ORD-004 should be quarantined, got count={quarantined_order}"
    assert quarantined_customer == 1, f"FAILED: CUST-005 should be quarantined, got count={quarantined_customer}"
    assert valid_orders_count > 0, "FAILED: Valid orders should still process despite bad data existing"
    
    # Confirm quarantine reason is populated (not silently dropped)
    reason = spark.table("quarantine_orders").filter("order_id = 'ORD-004'").select("quarantine_reason").collect()[0][0]
    assert reason is not None and reason != "", "FAILED: Quarantine reason should be populated"
    print(f"Quarantine reason for ORD-004: {reason}")
    
    print("TEST PASSED: Bad data quarantined, rest of batch succeeded")

test_quarantine()
