#!/bin/bash

set -e

MODULE=$1
MAVEN_CLI_OPTS="--batch-mode -s settings.xml"
RUN_TESTS_COMMAND="mvn $MAVEN_CLI_OPTS -pl $MODULE verify -Dsg.tests.es_download_cache.dir=$(pwd) -Dsg.tests.sg_plugin.file=$(realpath ./plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT*.zip) -Drevision=$SNAPSHOT_REVISION -Delasticsearch.version=$ES_VERSION"

useradd -m es_test
chown -R es_test .

if [ "$TEST_CLUSTER_TYPE" == "full" ]; then
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=true"
else
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=false"
fi

echo "RUN TEST COMMAND $RUN_TESTS_COMMAND"

su es_test -c "$RUN_TESTS_COMMAND"