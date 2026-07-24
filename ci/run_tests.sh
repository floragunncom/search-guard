#!/bin/bash
set -e

MODULE=$1
PROFILE=$2

# Resource/contention diagnostics (shared with the integration-test job). Written to the job log AND to an artifact
# under ci-diagnostics/. They track down flaky test-cluster startups (YELLOW cluster, "node not connected",
# "startup timed out"), which are almost always CPU/memory/IO contention when heavy jobs share a bare-metal runner.
source "$(dirname "$0")/resource-diagnostics.sh"
diag_init "$MODULE"

MAVEN_CLI_OPTS="--batch-mode -s settings.xml"
RUN_TESTS_COMMAND="mvn $MAVEN_CLI_OPTS -pl $MODULE test \
  -Dmaven.repo.local=$(pwd)/.m2/repository \
  -Dsg.tests.es_download_cache.dir=${ES_DL_CACHE:-$(pwd)} \
  -Dsg.tests.sg_plugin.file=$(realpath ./plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT*.zip) \
  -Drevision=$SNAPSHOT_REVISION \
  -Delasticsearch.version=$ES_VERSION"

if [ -n "$PROFILE" ]; then
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -P $PROFILE"
fi
useradd -m es_test

chown -R es_test .

if [ "$TEST_CLUSTER_TYPE" == "full" ]; then
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=true"
else
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=false"
fi

diag_snapshot "BEFORE TESTS"
diag_sampler_start

echo "RUN TEST COMMAND $RUN_TESTS_COMMAND"

# Run the tests without letting set -e abort before we print the closing diagnostics, then propagate the real exit code.
set +e
su - es_test -c "cd `pwd` && $RUN_TESTS_COMMAND"
TEST_EXIT=$?
set -e

diag_sampler_stop
diag_snapshot "AFTER TESTS"

echo "TEST EXIT CODE: $TEST_EXIT (see $DIAG_FILE, uploaded as a ci-diagnostics artifact)"
exit $TEST_EXIT
