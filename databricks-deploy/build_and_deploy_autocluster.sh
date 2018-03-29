#!/usr/bin/env bash
set -eux pipefail

source "$(dirname "$0")"/set_env.sh

DBFS_JAR_DIR=telemetry-streaming-lib
JAR_NAME=telemetry-streaming-assembly-0.1-SNAPSHOT.jar

create_job_json() {
cat << EOF
{
    "name": "Error aggregates test",
    "new_cluster": {
        "spark_version": "4.0.x-scala2.11",
        "node_type_id": "c3.4xlarge",
        "aws_attributes": {
            "availability": "SPOT_WITH_FALLBACK",
            "instance_profile_arn": "${IAM_ROLE}",
            "zone_id": "us-west-2b"
        },
        "autoscale": {
            "min_workers": 1,
            "max_workers": 8
        },
        "ssh_public_keys": ["${SSH_PUBLIC_KEY}"],
        "spark_conf": {
            "spark.metrics.namespace": "telemetry-streaming",
            "spark.metrics.conf.*.sink.statsd.class": "org.apache.spark.metrics.sink.StatsdSink",
            "spark.sql.streaming.metricsEnabled": "true"
        },
        "custom_tags": {
            "TelemetryJobName": "error_aggregates_test"
        }
    },
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
sbt clean assembly

echo "Deploying jar to Databricks cluster..."
databricks fs ls dbfs:/${DBFS_JAR_DIR} || databricks fs mkdirs dbfs:/${DBFS_JAR_DIR}
databricks fs ls dbfs:/${DBFS_JAR_DIR}/${JAR_NAME} && databricks fs rm dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}
databricks fs cp target/scala-2.11/${JAR_NAME} dbfs:/${DBFS_JAR_DIR}/${JAR_NAME}

echo "Creating job..."
databricks jobs create --json "$(create_job_json)"
#databricks jobs reset --job-id 71 --json "$(create_job_json)"