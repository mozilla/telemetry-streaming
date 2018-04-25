#!/bin/bash
curl -sL https://raw.githubusercontent.com/travis-ci/artifacts/master/install | bash
tar -C dist -zcvf all.tgz .

export ARTIFACTS_PERMISSIONS="public-read"
export ARTIFACTS_PATHS="$(find $TOP_LEVEL_JAR_DIR -type f | tr "\n" ":")"
artifacts upload
