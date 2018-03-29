#!/usr/bin/env bash
set -eux pipefail

source "$(dirname "$0")"/set_env.sh

create_cluster_json() {
cat << EOF
{
    "cluster_name": "Error aggregates test",
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
    }
}
EOF
}

databricks clusters create --json "$(create_cluster_json)"