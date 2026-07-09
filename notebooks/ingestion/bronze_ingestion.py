# Bronze Layer Ingestion: Customers, Orders, Products

import uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col

run_id = str(uuid.uuid4())
ingestion_ts = datetime.now().isoformat()

BASE_PATH = "/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/data/landing"

---------- CUSTOMERS (JSON) ----------
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
products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/products/products.csv")
    .withColumn("run_id", lit(run_id))
    .withColumn("ingestion_ts", lit(ingestion_ts))
)

products_df.write.format("delta").mode("append").saveAsTable("bronze_products")
print(f"Bronze products: {products_df.count()} rows")

print(f"\nRun ID: {run_id}")

## Verify tables landed in bronze layer correctly
print("Customers:", spark.table("bronze_customers").count())
print("Orders:", spark.table("bronze_orders").count())
print("Products:", spark.table("bronze_products").count())

spark.table("bronze_customers").show(5)
spark.table("bronze_orders").show(5)
spark.table("bronze_products").show(5)
