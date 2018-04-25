#!/bin/bash
curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash

export ARTIFACTS_PERMISSIONS="public-read"
export ARTIFACTS_PATHS="$TOP_LEVEL_JAR_DIR/"
export ARTIFACTS_TARGET_PATHS="/$TOP_LEVEL_JAR_DIR/"
artifacts upload
