# Silver Layer: Products

from pyspark.sql.functions import col, when, concat_ws, lit

bronze_products = spark.table("bronze_products")

validated = bronze_products.withColumn(
    "quarantine_reason",
    concat_ws("; ",
        when(col("product_id").isNull(), lit("missing_product_id")),
        when(col("unit_cost") < 0, lit("negative_unit_cost"))
    )
)

good_products = validated.filter(col("quarantine_reason") == "")
bad_products = validated.filter(col("quarantine_reason") != "")

good_products.drop("quarantine_reason").write.format("delta").mode("overwrite").saveAsTable("silver_products")
bad_products.write.format("delta").mode("append").saveAsTable("quarantine_products")

print(f"Silver products (valid): {good_products.count()}")
print(f"Quarantined products: {bad_products.count()}")