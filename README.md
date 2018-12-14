[![Build Status](https://circleci.com/gh/mozilla/telemetry-streaming/tree/master.svg?style=svg)](https://circleci.com/gh/mozilla/telemetry-streaming/tree/master)
[![codecov.io](https://codecov.io/github/mozilla/telemetry-streaming/coverage.svg?branch=master)](https://codecov.io/github/mozilla/telemetry-streaming?branch=master)

# telemetry-streaming
Spark Streaming ETL jobs for Mozilla Telemetry

This service currently contains jobs that aggregate error data
on 5 minute intervals. It is responsible for generating the (internal only)
`error_aggregates` and `experiment_error_aggregates` parquet tables at
Mozilla.

## Issue Tracking

Please file bugs related to the error aggregates streaming job in the
[Datasets: Error Aggregates](https://bugzilla.mozilla.org/enter_bug.cgi?product=Data%20Platform%20and%20Tools&component=Datasets%3A%20Error%20Aggregates) component.

## Deployment

The jobs defined in this repository are generally deployed as streaming jobs within
[our hosted Databricks account](https://docs.telemetry.mozilla.org/concepts/pipeline/data_pipeline_detail.html?highlight=databricks#databricks-managed-spark-analysis),
but some are deployed as periodic batch jobs via Airflow
using wrappers codified in
[telemetry-airflow](https://github.com/mozilla/telemetry-airflow)
that spin up EMR clusters whose configuration is governed by
[emr-bootstrap-spark](https://github.com/mozilla/emr-bootstrap-spark/).
Changes in production behavior that don't seem to correspond to changes
in this repository's code could be related to changes in those other projects.

## Amplitude Event Configuration

Some of the jobs defined in `telemetry-streaming` exist to transform telemetry events
and republish to [Amplitude](https://amplitude.com/) for further analysis.
Filtering and transforming events is accomplished via JSON configurations.
If you're creating or updating such a schema, see:

- [Amplitude event configuration docs](docs/amplitude)

## Development

The recommended workflow for running tests is to use your favorite editor for editing
the source code and running the tests via sbt. Some common invocations for sbt:

* `sbt test  # run the basic set of tests (good enough for most purposes)`
* `sbt "testOnly *ErrorAgg*"  # run the tests only for packages matching ErrorAgg`
* `sbt "testOnly *ErrorAgg* -- -z version"  # run the tests only for packages matching ErrorAgg, limited to test cases with "version" in them`
* `sbt dockerComposeTest  # run the docker compose tests (slow)`
* `sbt "dockerComposeTest -tags:DockerComposeTag" # run only tests with DockerComposeTag (while using docker)`
* `sbt scalastyle test:scalastyle  # run linter`
* `sbt ci  # run the full set of continuous integration tests`

Some tests need Kafka to run. If one prefers to run them via IDE, it's required to run the test cluster:
```bash
sbt dockerComposeUp
```
or via plain docker-compose:
```bash
export DOCKER_KAFKA_HOST=$(./docker_setup.sh)
docker-compose -f docker/docker-compose.yml up
```
It's also good to shut down the cluster afterwards:
```bash
sbt dockerComposeStop
```
