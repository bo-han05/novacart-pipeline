# Silver Layer: Customers

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/notebooks/utils/logging_helper.py").read())

from pyspark.sql.functions import col, when, concat_ws, lit, row_number, current_date
from pyspark.sql.window import Window
import uuid

VALID_TIERS = {"standard", "silver", "gold"}

run_id = str(uuid.uuid4())
start_time = log_step_start(run_id, "silver_customers")

bronze_customers = spark.table("bronze_customers")

# ---- Deduplicate: keep latest ingestion per customer_id ----
window = Window.partitionBy("customer_id").orderBy(col("ingestion_ts").desc())
deduped = (
    bronze_customers
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Validate if city and country exist
address_fields = {f.name for f in deduped.schema["address"].dataType.fields}
if not {"city", "country"}.issubset(address_fields):
    raise Exception(f"SCHEMA VALIDATION FAILED: address missing required fields. Found: {address_fields}")

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

good_customers.drop("quarantine_reason").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_customers")
bad_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("quarantine_customers")

row_count_out = good_customers.count()
quarantined_count = bad_customers.count()

print(f"Silver customers (valid): {row_count_out}")
print(f"Quarantined customers: {quarantined_count}")

# ---- End logging ----
log_step_end(
    run_id, "silver_customers", start_time,
    row_count_in=bronze_customers.count(),
    row_count_out=row_count_out,
    quarantined_count=quarantined_count
)
