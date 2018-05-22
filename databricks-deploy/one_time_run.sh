#!/usr/bin/env bash
set -eux pipefail

source "$(dirname "$0")"/set_env.sh

DBFS_JAR_DIR=telemetry-streaming-lib-v2-preview
JAR_NAME=telemetry-streaming-assembly-0.1-SNAPSHOT.jar

OUTPUT_PATH=s3://mozilla-databricks-telemetry-test/error_aggregates_v2_preview
CHECKPOINT_PATH=s3://mozilla-databricks-telemetry-test/error_aggregates_v2_preview_checkpoint


create_job_json() {
cat << EOF
{
    "run_name": "Error aggregates - v2 preview",
    "new_cluster": {
        "spark_version": "4.0.x-scala2.11",
        "node_type_id": "c3.2xlarge",
        "aws_attributes": {
            "first_on_demand": "1",
            "availability": "SPOT_WITH_FALLBACK",
            "instance_profile_arn": "${IAM_ROLE}",
            "zone_id": "us-west-2b"
        },
        "autoscale": {
            "min_workers": 1,
            "max_workers": 15
        },
        "ssh_public_keys": ["${SSH_PUBLIC_KEY}"]
    },
    "libraries": [{"jar": "dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}"}],
    "email_notifications": {
        "on_start": ["akomarzewski@mozilla.com"],
        "on_success": ["akomarzewski@mozilla.com"],
        "on_failure": ["akomarzewski@mozilla.com"]
    },
    "spark_jar_task": {
        "main_class_name": "com.mozilla.telemetry.streaming.ErrorAggregator",
        "parameters": ["--kafkaBroker","kafka-3.pipeline.us-west-2.prod.mozaws.net:6667", "--outputPath","${OUTPUT_PATH}", "--checkpointPath","${CHECKPOINT_PATH}"]
    }
}
EOF
}

echo "Building jar..."
#sbt clean assembly

echo "Deploying jar to Databricks cluster..."
#databricks fs ls dbfs:/${DBFS_JAR_DIR} || databricks fs mkdirs dbfs:/${DBFS_JAR_DIR}
#databricks fs ls dbfs:/${DBFS_JAR_DIR}/${JAR_NAME} && databricks fs rm dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}
#databricks fs cp target/scala-2.11/${JAR_NAME} dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}

#echo $(create_job_json)

curl -s \
    -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$(create_job_json)" \
    https://dbc-caf9527b-e073.cloud.databricks.com/api/2.0/jobs/runs/submit
