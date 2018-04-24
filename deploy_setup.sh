#!/bin/bash

JAR_DIR="mozilla/telemetry-streaming"
JAR_NAME="telemetry-streaming.jar"
TXT_NAME="telemetry-streaming.txt"
JAR="target/scala-2.11/telemetry-streaming-assembly-0.1-SNAPSHOT.jar"

sbt assembly

if [[ -z "$TRAVIS_TAG" ]]; then
    BRANCH_OR_TAG=$TRAVIS_BRANCH
    ID=$TRAVIS_JOB_NUMBER
else
    BRANCH_OR_TAG=tags
    ID=$TRAVIS_TAG
fi

CANONICAL_JAR="$JAR_DIR/$BRANCH_OR_TAG/$ID/$JAR_NAME"
echo $CANONICAL_JAR > $TXT_NAME

mkdir -p $JAR_DIR/$BRANCH_OR_TAG/$ID
cp $JAR $CANONICAL_JAR
cp $JAR "$JAR_DIR/$BRANCH_OR_TAG/$JAR_NAME"
cp $TXT_NAME "$JAR_DIR/$BRANCH_OR_TAG/$TXT_NAME"
