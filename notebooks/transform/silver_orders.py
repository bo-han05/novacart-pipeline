# Silver Layer: Orders

from pyspark.sql.functions import col, current_timestamp, lit, row_number
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

# ---- Validation flags ----
validated = (
    deduped
    .withColumn("total_amount", col("quantity") * col("unit_price"))
    .withColumn(
        "quarantine_reason",
        col("order_id").isNull().cast("string")
    )
)

# Build reason column properly
from pyspark.sql.functions import when, concat_ws

validated = deduped.withColumn("total_amount", col("quantity") * col("unit_price"))

validated = validated.withColumn(
    "quarantine_reason",
    concat_ws("; ",
        when(col("order_id").isNull(), lit("missing_order_id")),
        when(col("customer_id").isNull(), lit("missing_customer_id")),
        when(col("product_id").isNull(), lit("missing_product_id")),
        when(col("quantity") <= 0, lit("invalid_quantity")),
        when(col("unit_price") < 0, lit("negative_unit_price")),
        when(~col("status").isin(list(VALID_STATUSES)), lit("invalid_status"))
    )
)

good_orders = validated.filter(col("quarantine_reason") == "")
bad_orders = validated.filter(col("quarantine_reason") != "")

# ---- Write Silver ----
good_orders.drop("quarantine_reason").write.format("delta").mode("overwrite").saveAsTable("silver_orders")

# ---- Write Quarantine ----
bad_orders.write.format("delta").mode("append").saveAsTable("quarantine_orders")

print(f"Silver orders (valid): {good_orders.count()}")
print(f"Quarantined orders: {bad_orders.count()}")

bad_orders.select("order_id", "customer_id", "quantity", "unit_price", "status", "quarantine_reason").show(truncate=False)