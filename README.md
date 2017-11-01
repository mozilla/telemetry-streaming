[![Build Status](https://travis-ci.org/mozilla/telemetry-streaming.svg?branch=master)](https://travis-ci.org/mozilla/telemetry-streaming)
[![codecov.io](https://codecov.io/github/mozilla/telemetry-streaming/coverage.svg?branch=master)](https://codecov.io/github/mozilla/telemetry-streaming?branch=master)

# telemetry-streaming
Spark Streaming ETL jobs for Mozilla Telemetry

## Development

The recommended workflow for running tests is to use your favorite editor for editing
the source code and running the tests via sbt. Some common invocations for sbt:

* `sbt test  # run the basic set of tests (good enough for most purposes)`
* `sbt "testOnly *ErrorAgg*"  # run the tests only for packages matching ErrorAgg`
* `sbt "testOnly *ErrorAgg* -- -z version"  # run the tests only for packages matching ErrorAgg, limited to test cases with "version" in them`
* `sbt dockerComposeTest  # run the docker compose tests (slow)`
* `sbt "dockerComposeTest -tags:DockerComposeTag" # run only tests with DockerComposeTag (while using docker)`
* `sbt ci  # run all tests`
