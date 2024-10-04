#!/bin/bash

set -e

MODULE=$1
MAVEN_CLI_OPTS="--batch-mode -s settings.xml"

if [ "$TEST_CLUSTER_TYPE" == "full" ]; then
  useradd es_test
  chown -R es_test .
  su es_test -c "mvn $MAVEN_CLI_OPTS -pl $MODULE verify -Dsg.tests.use_ep_cluster=true -Dsg.tests.sg_plugin.file=$(realpath ./plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT*.zip) -Dsg.tests.es_download_cache.dir=$(pwd)"
else
  mvn $MAVEN_CLI_OPTS -Drevision=$SNAPSHOT_REVISION -Delasticsearch.version=$ES_VERSION -pl $MODULE verify
fi
