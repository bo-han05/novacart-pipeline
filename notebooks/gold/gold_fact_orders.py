# Gold Layer: fact_orders

from delta.tables import DeltaTable
from pyspark.sql.functions import col, date_format

silver_orders = spark.table("silver_orders")
dim_customer = spark.table("dim_customer").filter(col("is_current") == True)
dim_product = spark.table("dim_product")
dim_date = spark.table("dim_date")

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
