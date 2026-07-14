# Bronze Layer Ingestion: Customers, Orders, Products

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/logging_helper

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/schema_definitions

%run /Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/schema_validator

import uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col, max as spark_max
from functools import reduce

run_id = str(uuid.uuid4())
ingestion_ts = datetime.now().isoformat()

BASE_PATH = "/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/data/landing"

# ---------- CUSTOMERS (JSON) ----------
print("\nIngesting Customers")
start_time = log_step_start(run_id, "bronze_customers")

customers_df = (
    spark.read
    .option("multiLine", True)
    .json(f"{BASE_PATH}/customers/customers.json")
)

# Validate schema before adding metadata columns
validate_schema(customers_df, CUSTOMERS_REQUIRED, "customers")

customers_df = (
    customers_df
    .withColumn("run_id", lit(run_id))
    .withColumn("ingestion_ts", lit(ingestion_ts))
)

customers_df.write.format("delta").mode("append").saveAsTable("bronze_customers")
row_count_out=customers_df.count()
print(f"Bronze customers: {row_count_out} rows")

# ---- End logging ----
log_step_end(
    run_id, "bronze_customers", start_time,
    row_count_in=row_count_out,
    row_count_out=row_count_out,
    quarantined_count=0)

# ---------- ORDERS (CSV) ----------
print("\nIngesting Orders")
start_time = log_step_start(run_id, "bronze_orders")

# Read all CSV files in the orders directory and union with schema reconciliation
file_list = [f.path for f in dbutils.fs.ls(f"{BASE_PATH}/orders") if f.name.startswith("orders_") and f.name.endswith(".csv")]
dfs = []
for f in file_list:
    df = spark.read.option("header", True).option("inferSchema", True).csv(f)
    validate_schema(df, ORDERS_REQUIRED, f"orders ({f})")  # validate before merging
    df = df.withColumn("source_file", lit(f))
    dfs.append(df)
orders_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

orders_df = (
    orders_df
    .withColumn("run_id", lit(run_id))
    .withColumn("ingestion_ts", lit(ingestion_ts))
)

orders_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("bronze_orders")
row_count_out = orders_df.count()
print(f"Bronze orders: {orders_df.count()} rows")

# ---- End logging ----
log_step_end(
    run_id, "bronze_orders", start_time,
    row_count_in=row_count_out,
    row_count_out=row_count_out,
    quarantined_count=0)

# ---------- PRODUCTS (CSV, exported from SQLite) ----------
# products_df = (
#     spark.read
#     .option("header", True)
#     .option("inferSchema", True)
#     .csv(f"{BASE_PATH}/products/products.csv")
#     .withColumn("run_id", lit(run_id))
#     .withColumn("ingestion_ts", lit(ingestion_ts))
# )

# products_df.write.format("delta").mode("append").saveAsTable("bronze_products")
# print(f"Bronze products: {products_df.count()} rows")

# print(f"\nRun ID: {run_id}")

# ---------- PRODUCTS (Incremental) ----------
print("\nIngesting Products")
start_time = log_step_start(run_id, "bronze_products")

# Get last successful watermark for products
watermark_row = spark.sql("""
    SELECT last_watermark FROM pipeline_watermarks 
    WHERE source_name = "products"
    ORDER BY updated_at DESC LIMIT 1
""").collect()

last_watermark = watermark_row[0]["last_watermark"] if watermark_row else "1900-01-01T00:00:00"

print(f"Last watermark for products: {last_watermark}")

# Read full source, then filter to only changed rows
products_source = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/products/products.csv")
)

validate_schema(products_source, PRODUCTS_REQUIRED, "products")

products_df = (
    products_source
    .filter(col("updated_at") > lit(last_watermark))
    .withColumn("run_id", lit(run_id))
    .withColumn("ingestion_ts", lit(ingestion_ts))
)

new_row_count = products_df.count()
print(f"New/changed product rows since last watermark: {new_row_count}")

if new_row_count > 0:
    products_df.write.format("delta").mode("append").saveAsTable("bronze_products")

    # Update watermark to max updated_at from this batch
    new_watermark = products_df.agg(spark_max("updated_at")).collect()[0][0]

    spark.sql(f"""
        INSERT INTO pipeline_watermarks VALUES (
            "products", "{new_watermark}", current_timestamp()
        )
    """)
    print(f"Watermark updated to: {new_watermark}")
else:
    print("No new products to ingest — watermark unchanged")

# ---- End logging ----
log_step_end(
    run_id, "bronze_products", start_time,
    row_count_in=new_row_count,
    row_count_out=new_row_count,
    quarantined_count=0)

# Verify tables landed in bronze layer correctly
print("\nCustomers:", spark.table("bronze_customers").count())
print("Orders:", spark.table("bronze_orders").count())
print("Products:", spark.table("bronze_products").count())
