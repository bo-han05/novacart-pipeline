# Bronze Layer Ingestion: Customers, Orders, Products

import uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col, max as spark_max

run_id = str(uuid.uuid4())
ingestion_ts = datetime.now().isoformat()

BASE_PATH = "/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/data/landing"

# ---------- CUSTOMERS (JSON) ----------
customers_df = (
    spark.read
    .option("multiLine", True)
    .json(f"{BASE_PATH}/customers/customers.json")
    .withColumn("run_id", lit(run_id))
    .withColumn("ingestion_ts", lit(ingestion_ts))
)

customers_df.write.format("delta").mode("append").saveAsTable("bronze_customers")
print(f"Bronze customers: {customers_df.count()} rows")

# ---------- ORDERS (CSV) ----------
orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/orders/*.csv")
    .withColumn("run_id", lit(run_id))
    .withColumn("ingestion_ts", lit(ingestion_ts))
    .withColumn("source_file", col("_metadata.file_path"))
)

orders_df.write.format("delta").mode("append").saveAsTable("bronze_orders")
print(f"Bronze orders: {orders_df.count()} rows")

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

print(f"\nRun ID: {run_id}")

# ---------- PRODUCTS (Incremental) ----------
# Get last successful watermark for products
watermark_row = spark.sql("""
    SELECT last_watermark FROM pipeline_watermarks 
    WHERE source_name = "products"
    ORDER BY updated_at DESC LIMIT 1
""").collect()

last_watermark = watermark_row[0]["last_watermark"] if watermark_row else "1900-01-01T00:00:00"

print(f"\nLast watermark for products: {last_watermark}")

# Read full source, then filter to only changed rows
products_source = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/products/products.csv")
)

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
            'products', '{new_watermark}', current_timestamp()
        )
    """)
    print(f"Watermark updated to: {new_watermark}")
else:
    print("No new products to ingest — watermark unchanged")

# Verify tables landed in bronze layer correctly
print("\nCustomers:", spark.table("bronze_customers").count())
print("Orders:", spark.table("bronze_orders").count())
print("Products:", spark.table("bronze_products").count())
