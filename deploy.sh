#!/bin/bash
curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash

export ARTIFACTS_PERMISSIONS="public-read"
export ARTIFACTS_PATHS="$(find $TOP_LEVEL_JAR_DIR -type f | tr "\n" ":" | sed -e 's/:$//')"
artifacts upload
