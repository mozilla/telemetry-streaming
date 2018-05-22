#!/usr/bin/env bash
set -eux pipefail

source "$(dirname "$0")"/set_env.sh


echo "Building jar..."
sbt clean assembly

echo "Deploying jar to Databricks cluster..."
databricks fs ls dbfs:/${DBFS_JAR_DIR_DEV} || databricks fs mkdirs dbfs:/${DBFS_JAR_DIR_DEV}
databricks fs ls dbfs:/${DBFS_JAR_DIR_DEV}/${JAR_NAME} && databricks fs rm dbfs:/${DBFS_JAR_DIR_DEV}/${JAR_NAME}
databricks fs cp target/scala-2.11/${JAR_NAME} dbfs:/${DBFS_JAR_DIR_DEV}/${JAR_NAME}
