# Logging Helper Functions

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

def log_step_start(run_id, step_name):
    print(f"[{datetime.now()}] START  | run_id={run_id} | step={step_name}")
    return datetime.now()

def log_step_end(run_id, step_name, start_time, row_count_in, row_count_out, quarantined_count=0, status="SUCCESS", error_message=None):
    spark = SparkSession.builder.getOrCreate()
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"[{end_time}] END    | run_id={run_id} | step={step_name} | "
          f"in={row_count_in} out={row_count_out} quarantined={quarantined_count} | "
          f"duration={duration:.2f}s | status={status}")
    
    spark.sql(f"""
        INSERT INTO pipeline_run_steps VALUES (
            '{run_id}', '{step_name}', {row_count_in}, {row_count_out}, {quarantined_count},
            '{start_time}', '{end_time}', '{status}', {f"'{error_message}'" if error_message else 'NULL'}
        )
    """)

def start_pipeline_run(run_id):
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"""
        INSERT INTO pipeline_runs VALUES (
            '{run_id}', current_timestamp(), NULL, 'RUNNING', NULL
        )
    """)
    print(f"Pipeline run started: {run_id}")

def end_pipeline_run(run_id, status="SUCCESS", error_message=None):
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"""
        UPDATE pipeline_runs 
        SET completed_at = current_timestamp(), 
            status = '{status}',
            error_message = {f"'{error_message}'" if error_message else 'NULL'}
        WHERE run_id = '{run_id}'
    """)
    print(f"Pipeline run ended: {run_id} | status={status}")
