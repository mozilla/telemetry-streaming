#!/usr/bin/env bash
set -eux pipefail

source "$(dirname "$0")"/set_env.sh

DBFS_JAR_DIR=telemetry-streaming-lib
JAR_NAME=telemetry-streaming-assembly-0.1-SNAPSHOT.jar

create_one_time_run_json() {
cat << EOF
{
    "run_name": "Error aggregates test run",
    "existing_cluster_id": "0329-095608-tens7",
    "libraries": [{"jar": "dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}"}],
    "email_notifications": {
        "on_start": [],
        "on_success": [],
        "on_failure": []
    },
    "spark_jar_task": {
        "main_class_name": "com.mozilla.telemetry.streaming.ErrorAggregator",
        "parameters": ["--kafkaBroker","${KAFKA_BROKER}", "--outputPath","${OUTPUT_PATH}", "--checkpointPath","${CHECKPOINT_PATH}"]
    }
}
EOF
}

databricks jobs create --json "$(create_job_json)"
