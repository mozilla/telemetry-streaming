[![Build Status](https://travis-ci.org/mozilla/telemetry-streaming.svg?branch=master)](https://travis-ci.org/mozilla/telemetry-streaming)
[![codecov.io](https://codecov.io/github/mozilla/telemetry-streaming/coverage.svg?branch=master)](https://codecov.io/github/mozilla/telemetry-streaming?branch=master)

# telemetry-streaming
Spark Streaming ETL jobs for Mozilla Telemetry

This service currently contains jobs that aggregate error aggregate data
on 5 minute intervals. It is responsible for generating the (internal only)
`error_aggregates` and `experiment_error_aggregates` parquet tables at
Mozilla.

## Issue Tracking

Please file bugs in the [Datasets: Error Aggregates](https://bugzilla.mozilla.org/enter_bug.cgi?product=Data%20Platform%20and%20Tools&component=Datasets%3A%20Error%20Aggregates) component.

## Development

The recommended workflow for running tests is to use your favorite editor for editing
the source code and running the tests via sbt. Some common invocations for sbt:

* `sbt test  # run the basic set of tests (good enough for most purposes)`
* `sbt "testOnly *ErrorAgg*"  # run the tests only for packages matching ErrorAgg`
* `sbt "testOnly *ErrorAgg* -- -z version"  # run the tests only for packages matching ErrorAgg, limited to test cases with "version" in them`
* `sbt dockerComposeTest  # run the docker compose tests (slow)`
* `sbt "dockerComposeTest -tags:DockerComposeTag" # run only tests with DockerComposeTag (while using docker)`
* `sbt ci  # run all tests`
