# Record end full pipeline run

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

pipeline_run_id = dbutils.jobs.taskValues.get(taskKey="job_start", key="pipeline_run_id")
end_pipeline_run(pipeline_run_id, status="SUCCESS")