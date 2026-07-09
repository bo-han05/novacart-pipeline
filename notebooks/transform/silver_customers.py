# Silver Layer: Customers

from pyspark.sql.functions import col, when, concat_ws, lit

bronze_customers = spark.table("bronze_customers")

flattened = bronze_customers.select(
    "customer_id", "first_name", "last_name", "email",
    col("address.city").alias("city"),
    col("address.country").alias("country"),
    "signup_date", "tier", "run_id", "ingestion_ts"
)

validated = flattened.withColumn(
    "quarantine_reason",
    concat_ws("; ",
        when(col("customer_id").isNull(), lit("missing_customer_id")),
        when(~col("email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w+$"), lit("invalid_email"))
    )
)

good_customers = validated.filter(col("quarantine_reason") == "")
bad_customers = validated.filter(col("quarantine_reason") != "")

good_customers.drop("quarantine_reason").write.format("delta").mode("overwrite").saveAsTable("silver_customers")
bad_customers.write.format("delta").mode("append").saveAsTable("quarantine_customers")

print(f"Silver customers (valid): {good_customers.count()}")
print(f"Quarantined customers: {bad_customers.count()}")