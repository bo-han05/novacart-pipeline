# Gold Layer: fact_orders

from delta.tables import DeltaTable
from pyspark.sql.functions import col, date_format, lit

silver_orders = spark.table("silver_orders")
dim_customer = spark.table("dim_customer").filter(col("is_current") == True)
dim_product = spark.table("dim_product")
dim_date = spark.table("dim_date")

# ---- Reusable referential integrity checker ----
def check_orphans(source_df, dim_df, join_col, reason):
    orphaned = (
        source_df.join(dim_df, join_col, "left_anti")
        .withColumn("quarantine_reason", lit(reason))
    )
    count = orphaned.count()
    if count > 0:
        print(f"WARNING: {count} rows failed check — {reason}")
    else:
        print(f"No issues found — {reason}")
    return orphaned

# ---- Run all referential checks before building fact table ----
customer_orphans = check_orphans(silver_orders, dim_customer, "customer_id", 
              "customer_not_found_in_dimension")

product_orphans = check_orphans(silver_orders, dim_product, "product_id", 
              "product_not_found_in_dimension")

all_orphans = customer_orphans.union(product_orphans)
if all_orphans.count() > 0:
    all_orphans.write.format("delta").mode("overwrite").saveAsTable("quarantine_fact_orders")

# ---- Build fact table (only rows with valid dimension keys survive the joins anyway) ----
fact = (
    silver_orders.alias("o")
    .join(dim_customer.alias("c"), col("o.customer_id") == col("c.customer_id"))
    .join(dim_product.alias("p"), col("o.product_id") == col("p.product_id"))
    .withColumn("date_sk", date_format(col("o.order_date"), "yyyyMMdd").cast("int"))
    .select(
        col("o.order_id"),
        col("c.customer_id").alias("customer_id"),
        col("p.product_id").alias("product_id"),
        col("date_sk"),
        col("o.quantity"),
        col("o.unit_price"),
        col("o.total_amount"),
        col("o.status"),
        col("o.run_id")
    )
)

if not spark.catalog.tableExists("fact_orders"):
    fact.write.format("delta").saveAsTable("fact_orders")
    print(f"fact_orders created: {fact.count()} rows")
else:
    target = DeltaTable.forName(spark, "fact_orders")
    (
        target.alias("t")
        .merge(fact.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"fact_orders merged. Total rows: {spark.table('fact_orders').count()}")
