# Gold Layer: dim_date

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

from pyspark.sql.functions import col, explode, sequence, to_date, year, month, dayofmonth, weekofyear, quarter, dayofweek, date_format
import uuid

run_id = str(uuid.uuid4())
start_time = log_step_start(run_id, "gold_dim_date")

date_range = spark.sql("""
    SELECT min(order_date) as min_date, max(order_date) as max_date FROM silver_orders
""").collect()[0]

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{date_range['min_date']}'), to_date('{date_range['max_date']}'), interval 1 day)) as full_date")
    .withColumn("date_sk", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("year", year(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("week", weekofyear(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dim_date")
row_count_out = dim_date.count()
print(f"dim_date rows: {row_count_out}")

# ---- End logging ----
log_step_end(
    run_id, "gold_dim_date", start_time,
    row_count_in=row_count_out,
    row_count_out=row_count_out,
    quarantined_count=0
)
