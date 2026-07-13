# Gold Layer: dim_product (SCD1)

from delta.tables import DeltaTable
from pyspark.sql.functions import col

silver_products = spark.table("silver_products")

if not spark.catalog.tableExists("dim_product"):
    dim_product = silver_products.select(
        "product_id", "name", "category", "unit_cost", "supplier_id", "updated_at"
    )
    dim_product.write.format("delta").saveAsTable("dim_product")
    print(f"dim_product created: {dim_product.count()} rows")
else:
    target = DeltaTable.forName(spark, "dim_product")
    (
        target.alias("t")
        .merge(silver_products.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdate(set={
            "name": "s.name",
            "category": "s.category",
            "unit_cost": "s.unit_cost",
            "supplier_id": "s.supplier_id",
            "updated_at": "s.updated_at"
        })
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
    print(f"dim_product merged. Total rows: {spark.table('dim_product').count()}")
