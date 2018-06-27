#!/usr/bin/env python

import dbjobs
import time

# Json specification for a databricks job
# See cloudops-deployment/projects/databricks/jsonnet for examples and README
JOB_CONF = 'YOUR-JOB-CONFIG.json'

# Create a new job from json config file
job_id = dbjobs.create_job(JOB_CONF)
print('job_id created is {}'.format(job_id))

# Update an existing job from json config file
dbjobs.reset_config(job_id, JOB_CONF)

# Start and stop an existing job
test_job_id = <YOUR JOB ID>
run_id = dbjobs.start_job(test_job_id)
print('started job {}, run id is {}'.format(test_job_id, run_id))
time.sleep(30)
dbjobs.stop_job(test_job_id)

# Fetch a job id using existing custom tags: TelemetryJobName, Env, Jar, DatasetVersion
# Note that the job must be started by the JENKINS_USER specified in dbjobs for this to work. 
get_job_id = dbjobs.get_jobid('harold test', 'databricks-dev', 's3://net-mozaws-data-us-west-2-ops-ci-artifacts/mozilla/telemetry-streaming/tags/v1.0.3/telemetry-streaming-doesnotexist.jar', 'v2')
print('get job id was {}'.format(get_job_id))
