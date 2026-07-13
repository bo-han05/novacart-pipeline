# Setup metadata tables

spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    source_name STRING,
    last_watermark STRING,
    updated_at TIMESTAMP
)
""")

print("Metadata tables ready")