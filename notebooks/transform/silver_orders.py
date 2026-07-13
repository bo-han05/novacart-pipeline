# Silver Layer: Orders

from pyspark.sql.functions import col, when, concat_ws, lit, row_number, current_date, round as spark_round
from pyspark.sql.window import Window

VALID_STATUSES = {"pending", "shipped", "delivered", "refunded"}

bronze_orders = spark.table("bronze_orders")

# ---- Deduplicate: keep latest ingestion per order_id ----
window = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())
deduped = (
    bronze_orders
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ---- Derived column ----
deduped = deduped.withColumn("total_amount", spark_round(col("quantity") * col("unit_price"), 2))

# ---- Validation ----
validated = deduped.withColumn(
    "quarantine_reason",
    concat_ws("; ",
        when(col("order_id").isNull(), lit("missing_order_id")),
        when(col("customer_id").isNull(), lit("missing_customer_id")),
        when(col("product_id").isNull(), lit("missing_product_id")),
        when(col("order_date").isNull(), lit("missing_order_date")),
        when(col("order_date") > current_date(), lit("future_order_date")),
        when(col("quantity") <= 0, lit("invalid_quantity")),
        when(col("unit_price") < 0, lit("negative_unit_price")),
        when(~col("status").isin(list(VALID_STATUSES)), lit("invalid_status"))
    )
)

good_orders = validated.filter(col("quarantine_reason") == "")
bad_orders = validated.filter(col("quarantine_reason") != "")

good_orders.drop("quarantine_reason").write.format("delta").mode("overwrite").saveAsTable("silver_orders")
bad_orders.write.format("delta").mode("overwrite").saveAsTable("quarantine_orders")

print(f"Silver orders (valid): {good_orders.count()}")
print(f"Quarantined orders: {bad_orders.count()}")
