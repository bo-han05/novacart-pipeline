# Gold Layer: dim_date

from pyspark.sql.functions import col, explode, sequence, to_date, year, month, dayofmonth, weekofyear, quarter, dayofweek, date_format

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

dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")
print(f"dim_date rows: {dim_date.count()}")
