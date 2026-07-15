# Gold Layer: dim_product (SCD1)

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

from delta.tables import DeltaTable
from pyspark.sql.functions import col
import uuid

run_id = str(uuid.uuid4())
start_time = log_step_start(run_id, "gold_dim_product")

silver_products = spark.table("silver_products")

if not spark.catalog.tableExists("dim_product"):
    dim_product = silver_products.select(
        "product_id", "name", "category", "unit_cost", "supplier_id", "updated_at"
    )
    dim_product.write.format("delta").saveAsTable("dim_product")
    row_count_out = dim_product.count()
    print(f"dim_product created: {row_count_out} rows")
else:
    target = DeltaTable.forName(spark, "dim_product")
    (
        target.alias("t")
        .merge(silver_products.alias("s"), "t.product_id = s.product_id")
        # if product already exists -> overwrite its attributes with latest values
        .whenMatchedUpdate(set={
            "name": "s.name",
            "category": "s.category",
            "unit_cost": "s.unit_cost",
            "supplier_id": "s.supplier_id",
            "updated_at": "s.updated_at"
        })
        # if product is new -> insert it as a new row
        .whenNotMatchedInsert(values={
            "product_id": "s.product_id",
            "name": "s.name",
            "category": "s.category",
            "unit_cost": "s.unit_cost",
            "supplier_id": "s.supplier_id",
            "updated_at": "s.updated_at"
        })
        .execute()
    )
    row_count_out = spark.table("dim_product").count()
    print(f"dim_product merged. Total rows: {row_count_out}")

# ---- End logging ----
log_step_end(
    run_id, "gold_dim_product", start_time,
    row_count_in=silver_products.count(),
    row_count_out=row_count_out,
    quarantined_count=0
)
