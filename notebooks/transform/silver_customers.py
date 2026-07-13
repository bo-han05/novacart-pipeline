# Silver Layer: Customers

from pyspark.sql.functions import col, when, concat_ws, lit, row_number, current_date
from pyspark.sql.window import Window

VALID_TIERS = {"standard", "silver", "gold"}

bronze_customers = spark.table("bronze_customers")

# ---- Deduplicate: keep latest ingestion per customer_id ----
window = Window.partitionBy("customer_id").orderBy(col("ingestion_ts").desc())
deduped = (
    bronze_customers
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Flatten nested address ----
flattened = deduped.select(
    "customer_id", "first_name", "last_name", "email",
    col("address.city").alias("city"),
    col("address.country").alias("country"),
    "signup_date", "tier", "run_id", "ingestion_ts"
)

# ---- Validation ----
validated = flattened.withColumn(
    "quarantine_reason",
    concat_ws("; ",
        when(col("customer_id").isNull(), lit("missing_customer_id")),
        when(col("first_name").isNull(), lit("missing_first_name")),
        when(col("last_name").isNull(), lit("missing_last_name")),
        when(col("city").isNull(), lit("missing_city")),
        when(col("country").isNull(), lit("missing_country")),
        when(~col("tier").isin(list(VALID_TIERS)), lit("invalid_tier")),
        when(col("signup_date").isNull(), lit("missing_signup_date")),
        when(col("signup_date") > current_date(), lit("future_signup_date")),
        when(~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lit("invalid_email"))
    )
)

good_customers = validated.filter(col("quarantine_reason") == "")
bad_customers = validated.filter(col("quarantine_reason") != "")

good_customers.drop("quarantine_reason").write.format("delta").mode("overwrite").saveAsTable("silver_customers")
bad_customers.write.format("delta").mode("overwrite").saveAsTable("quarantine_customers")

print(f"Silver customers (valid): {good_customers.count()}")
print(f"Quarantined customers: {bad_customers.count()}")
