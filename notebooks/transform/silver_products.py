# Silver Layer: Products

from pyspark.sql.functions import col, when, concat_ws, lit, row_number, current_timestamp
from pyspark.sql.window import Window

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

good_products.drop("quarantine_reason").write.format("delta").mode("overwrite").saveAsTable("silver_products")
bad_products.write.format("delta").mode("overwrite").saveAsTable("quarantine_products")

print(f"Silver products (valid): {good_products.count()}")
print(f"Quarantined products: {bad_products.count()}")
