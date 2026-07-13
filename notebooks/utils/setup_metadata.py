# Setup metadata tables

spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    source_name STRING,
    last_watermark STRING,
    updated_at TIMESTAMP
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status STRING,
    error_message STRING
)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_run_steps (
    run_id STRING,
    step_name STRING,
    row_count_in INT,
    row_count_out INT,
    quarantined_count INT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status STRING,
    error_message STRING
)
""")

print("Metadata tables ready")
