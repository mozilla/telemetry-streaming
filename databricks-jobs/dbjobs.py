#!/usr/bin/env python

import base64
import json
import os
from polling import TimeoutException, poll
import requests

DOMAIN = os.getenv('DATABRICKS_URL', 'dbc-caf9527b-e073.cloud.databricks.com')
JOBS_BASE_URL = 'https://%s/api/2.0/jobs/' % (DOMAIN)
RUNS_BASE_URL = 'https://%s/api/2.0/jobs/runs/' % (DOMAIN)
JENKINS_USER = 'dataops+databrick_jenkins@mozilla.com'
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']


def jobs_rpc(action, jsonstring):
    """ A helper function to make the JOBS API POST request, request/response is encoded/decoded as JSON

    Args:
        action: The jobs rest api endpoint e.g. create, list, delete, get, reset, run-now
        jsonstring: String representation of json payload to POST to the jobs REST api

    Returns:
        dict holding json response

    """
    response = requests.post(
        JOBS_BASE_URL + action,
        headers={"Authorization": "Basic " + base64.standard_b64encode("token:" + DATABRICKS_TOKEN)},
        data=jsonstring
    )
    return response.json()


def runs_rpc(action, jsonstring):
    """ A helper function to make the job RUNS API GET request, request/response is encoded/decoded as JSON

    Args:
        action: The job runs rest api endpoint e.g. list, get, export, cancel, get-output, delete
        jsonstring: String representation of json payload to POST to the job runs REST api

    Returns:
        dict holding json response

    """
    response = requests.get(
        RUNS_BASE_URL + action,
        headers={"Authorization": "Basic " + base64.standard_b64encode("token:" + DATABRICKS_TOKEN)},
        data=jsonstring
    )
    return response.json()


def reset_config(jobid, jsonfile):
    """ Resets job config for jobid to contents of jsonfile

    Args:
        jobid: The id of job to modify
        jsonfile: Path to the file containing json job definition

    """
    with open(jsonfile) as f:
        job_settings_map = json.load(f)

    data = {
        'job_id': jobid,
        'new_settings': job_settings_map
    }

    json_string = json.dumps(data)
    print('Attempting to update jobid {} with settings {}'.format(jobid, json_string))
    jobs_rpc('reset', json_string)


def create_job(jsonfile):
    """ Creates a new job

    Args:
        jsonfile: Path to the file containing json job definition

    Returns:
        jobid: The new job's id number

    """
    with open(jsonfile) as f:
        job_settings_map = json.load(f)
    job_settings_string = json.dumps(job_settings_map)
    response_json = jobs_rpc('create', job_settings_string)
    return response_json['job_id']


def stop_job(job_id):
    """ Stops all runs of a job, polls until stopped

    Args:
        job_id: The id of the job to stop all runs for

    """
    data = {
        'active_only': 'true',
        'job_id': job_id,
        'limit': 0
    }

    json_string = json.dumps(data)
    response_json = runs_rpc('list', json_string)

    if 'runs' not in response_json:
        print('job_id {} had no runs'.format(job_id))
        return

    runids = []
    for run in response_json['runs']:
        runids.append(run['run_id'])

    for runid in runids:
        print('Asynchronously cancelling run_id {} for job_id {}'.format(runid, job_id))
        request = {}
        request['run_id'] = runid
        request_string = json.dumps(request)

        response = requests.post(
            RUNS_BASE_URL + 'cancel',
            headers={"Authorization": "Basic " + base64.standard_b64encode("token:" + DATABRICKS_TOKEN)},
            data=request_string
            )

        response_dict = response.json()
        # If response dict is non empty, print contents which contain error
        if response_dict:
            print('Cancel job call has likely failed. Response was nonempty: {}'.format(str(response_dict)))
            return

        print('Poll until run_id {} is stopped'.format(runid))
        try:
            poll(
                lambda: poll_job_completed(request_string) == True,
                step=10,
                timeout=300
                )
            print('Run_id {} is in a terminal state'.format(runid))
        except TimeoutException:
            print('Timed out waiting for run_id {} to terminate'.format(runid))


def poll_job_completed(request_string):
    """ Helper function for polling until the job is in a terminal state """
    terminal_state = ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']
    response_json = runs_rpc('get', request_string)
    return True if (response_json['state']['life_cycle_state'] in terminal_state) else False


def start_job(job_id):
    """ Starts a stopped job

    Args:
        job_id: The id of the job to start

    Returns:
        run_id: The id of the new job run, or -1 if it failed

    """
    data = {
        'job_id': job_id
    }

    data_string = json.dumps(data)
    response_json = jobs_rpc('run-now', data_string)
    if 'run_id' not in response_json:
        print('failed to start job {}, response was {}'.format(job_id, str(response_json)))
        return -1
    return response_json['run_id']


def get_jobid(jobname, env, jarname, version):
    """ Gets the job id of a databricks job deployed given its jobname and env, jarname and version custom tags

    Custom tags are not viewable from the databricks UI. Can be seen in the job json or from the API.

    Args:
        jobname: The custom tag "TelemetryJobName" defined in the job json
        env:     The custom tag "Env" defined in the job json
        jarname: The custom tag "Jar" defined in the job json
        version: The custom tag "DatasetVersion" defined in the job json

    Returns:
        job_id: The id of the job found.  0 if no job found, and -1 if more than one job found.

    """
    job_ids = []
    response = requests.get(
        JOBS_BASE_URL + 'list',
        headers={"Authorization": "Basic " + base64.standard_b64encode("token:" + DATABRICKS_TOKEN)}
    )
    jobs_list = response.json()

    for job in jobs_list['jobs']:
        if 'creator_user_name' not in job:
            continue
        elif job['creator_user_name'] != JENKINS_USER:
            continue
        elif 'settings' in job:
            if 'new_cluster' in job['settings']:
                if 'custom_tags' in job['settings']['new_cluster']:
                    tags = job['settings']['new_cluster']['custom_tags']
                    if 'Env' in tags and 'DatasetVersion' in tags and 'Jar' in tags and 'TelemetryJobName' in tags:
                        if tags['Env'] == env and tags['DatasetVersion'] == version and tags['Jar'] == jarname and tags['TelemetryJobName'] == jobname:
                            job_ids.append(job['job_id'])
        else:
            continue

    if (len(job_ids) == 1):
        return job_ids[0]
    elif (len(job_ids) < 1):
        print('GetJobId didnt find any jobs matching env: {}, jarname: {}, version: {}'.format(env, jarname, version))
        return 0
    else:
        print('GetJobId found more than 1 jobs matching env: {}, jarname: {}, version: {}'.format(env, jarname, version))
        print "Jobs found were: " + str(job_ids)
        return -1
