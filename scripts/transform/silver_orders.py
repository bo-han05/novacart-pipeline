# Silver Layer: Orders

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

from pyspark.sql.functions import col, when, concat_ws, lit, row_number, current_date, round as spark_round
from pyspark.sql.window import Window
import uuid

VALID_STATUSES = {"pending", "shipped", "delivered", "refunded"}

run_id = str(uuid.uuid4())
start_time = log_step_start(run_id, "silver_orders")

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

good_orders.drop("quarantine_reason").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
bad_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_orders")

row_count_out = good_orders.count()
quarantined_count = bad_orders.count()

print(f"Silver orders (valid): {row_count_out}")
print(f"Quarantined orders: {quarantined_count}")

# ---- End logging ----
log_step_end(
    run_id, "silver_orders", start_time,
    row_count_in=bronze_orders.count(),
    row_count_out=row_count_out,
    quarantined_count=quarantined_count
)
