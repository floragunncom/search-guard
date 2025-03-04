#!/bin/bash

set -e

MODULE=$1
MAVEN_CLI_OPTS="--batch-mode -s settings.xml"
RUN_TESTS_COMMAND="mvn $MAVEN_CLI_OPTS -pl $MODULE test -Dsg.tests.es_download_cache.dir=$(pwd) -Dsg.tests.sg_plugin.file=$(realpath ./plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT*.zip) -Drevision=$SNAPSHOT_REVISION -Delasticsearch.version=$ES_VERSION"

useradd -m es_test

# workaround related to downloading ES, download from java code results in incomplete file
wget -q https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-9.0.0-beta1-linux-x86_64.tar.gz
ES_HASH=$(sha512sum elasticsearch-9.0.0-beta1-linux-x86_64.tar.gz)
ES_LOCATION=$(realpath elasticsearch-9.0.0-beta1-linux-x86_64.tar.gz)
echo "Downloaded ES sha512sum is $ES_HASH, ES is stored in location $ES_LOCATION"


chown -R es_test .

if [ "$TEST_CLUSTER_TYPE" == "full" ]; then
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=true"
else
  RUN_TESTS_COMMAND="$RUN_TESTS_COMMAND -Dsg.tests.use_ep_cluster=false"
fi

echo "RUN TEST COMMAND $RUN_TESTS_COMMAND"

su es_test -c "$RUN_TESTS_COMMAND"