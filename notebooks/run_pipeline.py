# NovaCart Pipeline - Master Orchestrator

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/logging_helper
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/schema_definitions
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/schema_validator

import uuid

pipeline_run_id = str(uuid.uuid4())
start_pipeline_run(pipeline_run_id)
print(f"Pipeline Run Starting - Run ID: {pipeline_run_id}")

print("\n--- STAGE 1: BRONZE INGESTION ---")
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/ingestion/bronze_ingestion
print("\n--- STAGE 2: SILVER TRANSFORMATION ---")
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_orders
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_customers
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/transform/silver_products
print("\n--- STAGE 3: GOLD LAYER ---")
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_date
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_product
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_dim_customer
%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/gold/gold_fact_orders

end_pipeline_run(pipeline_run_id, status="SUCCESS")

print(f"Pipeline Run Complete - Status: SUCCESS")
print("\nFinal row counts:")
print(f"  fact_orders: {spark.table('fact_orders').count()}")
print(f"  dim_customer (current): {spark.table('dim_customer').filter('is_current = true').count()}")
print(f"  dim_product: {spark.table('dim_product').count()}")
