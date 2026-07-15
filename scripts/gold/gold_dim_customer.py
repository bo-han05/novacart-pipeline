# Gold Layer: dim_customer (SCD2)

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp
import uuid

run_id = str(uuid.uuid4())
start_time = log_step_start(run_id, "gold_dim_customer")

silver_customers = spark.table("silver_customers")

TRACKED_COLS = ["first_name", "last_name", "email", "city", "country", "tier"]

if not spark.catalog.tableExists("dim_customer"):
    dim_customer = (
        silver_customers
        .select("customer_id", *TRACKED_COLS, "signup_date")
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )
    dim_customer.write.format("delta").saveAsTable("dim_customer")
    row_count_out = dim_customer.count()
    print(f"dim_customer created: {row_count_out} rows")
else:
    target = DeltaTable.forName(spark, "dim_customer")
    current_rows = target.toDF().filter(col("is_current") == True)

    # find changed customers (compare tracked columns)
    change_condition = " OR ".join([f"t.{c} <> s.{c}" for c in TRACKED_COLS])

    changed = (
        silver_customers.alias("s")
        .join(current_rows.alias("t"), "customer_id")
        .where(change_condition)
        .select("s.customer_id")
    )

    new_customers = (
        silver_customers.alias("s")
        .join(current_rows.alias("t"), "customer_id", "left_anti")
    )

    # step 1: close out changed records
    if changed.count() > 0:
        changed_ids = [row["customer_id"] for row in changed.collect()]
        target.update(
            condition=(col("customer_id").isin(changed_ids)) & (col("is_current") == True),
            set={"is_current": lit(False), "effective_to": current_timestamp()}
        )

    # step 2: insert new + changed customer versions
    to_insert = silver_customers.join(changed, "customer_id", "inner").union(new_customers)
    to_insert = (
        to_insert
        .select("customer_id", *TRACKED_COLS, "signup_date")
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

    to_insert.write.format("delta").mode("append").saveAsTable("dim_customer")
    row_count_out = to_insert.count()
    print(f"dim_customer updated. New/changed rows inserted: {row_count_out}")

# ---- End logging ----
log_step_end(
    run_id, "gold_dim_customer", start_time,
    row_count_in=silver_customers.count(),
    row_count_out=row_count_out,
    quarantined_count=0
)
