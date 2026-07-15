# Silver Layer: Products

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

from pyspark.sql.functions import col, when, concat_ws, lit, row_number, current_timestamp
from pyspark.sql.window import Window
# import uuid

run_id = dbutils.jobs.taskValues.get(taskKey="job_start", key="pipeline_run_id")
## uncomment if running separately
# run_id = str(uuid.uuid4())
start_time = log_step_start(run_id, "silver_products")

bronze_products = spark.table("bronze_products")

# ---- Deduplicate: keep latest ingestion per product_id ----
window = Window.partitionBy("product_id").orderBy(col("ingestion_ts").desc())
deduped = (
    bronze_products
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ---- Validation ----
validated = deduped.withColumn(
    "quarantine_reason",
    concat_ws("; ",
        when(col("product_id").isNull(), lit("missing_product_id")),
        when(col("name").isNull(), lit("missing_name")),
        when(col("category").isNull(), lit("missing_category")),
        when(col("supplier_id").isNull(), lit("missing_supplier_id")),
        when(col("unit_cost") < 0, lit("negative_unit_cost")),
        when(col("updated_at") > current_timestamp(), lit("future_updated_at"))
    )
)

good_products = validated.filter(col("quarantine_reason") == "")
bad_products = validated.filter(col("quarantine_reason") != "")

good_products.drop("quarantine_reason").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_products")
bad_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_products")

row_count_out = good_products.count()
quarantined_count = bad_products.count()

print(f"Silver products (valid): {row_count_out}")
print(f"Quarantined products: {quarantined_count}")

# ---- End logging ----
log_step_end(
    run_id, "silver_products", start_time,
    row_count_in=bronze_products.count(),
    row_count_out=row_count_out,
    quarantined_count=quarantined_count
)
