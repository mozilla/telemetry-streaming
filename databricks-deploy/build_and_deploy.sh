#!/usr/bin/env bash
set -eux pipefail

source "$(dirname "$0")"/set_env.sh

DBFS_JAR_DIR=telemetry-streaming-lib
JAR_NAME=telemetry-streaming-assembly-0.1-SNAPSHOT.jar

create_job_json() {
cat << EOF
{
    "name": "Error aggregates test",
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

echo "Building jar..."
#sbt clean assembly

echo "Deploying jar to Databricks cluster..."
databricks fs ls dbfs:/${DBFS_JAR_DIR} || databricks fs mkdirs dbfs:/${DBFS_JAR_DIR}
databricks fs ls dbfs:/${DBFS_JAR_DIR}/${JAR_NAME} && databricks fs rm dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}
databricks fs cp target/scala-2.11/${JAR_NAME} dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}

echo "Creating job..."
#databricks jobs create --json "$(create_job_json)"
databricks jobs reset --job-id 71 --json "$(create_job_json)"