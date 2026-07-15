# Record start full pipeline run

exec(open("/Workspace/Repos/hanbo@ibm.com/novacart-pipeline/scripts/utils/logging_helper.py").read())

import uuid

pipeline_run_id = str(uuid.uuid4())
start_pipeline_run(pipeline_run_id)

dbutils.jobs.taskValues.set(key="pipeline_run_id", value=pipeline_run_id)

print(f"Shared run_id: {pipeline_run_id}")