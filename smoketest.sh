#!/bin/bash

set -e

MAIN_DIR=$(echo ~/searchguard-test)
DOWNLOAD_CACHE="$MAIN_DIR/download-cache"
INSTALL_DIR="$MAIN_DIR/es"
REPO_DIR=$(pwd)

if [ ! -d "$REPO_DIR/plugin/target/releases" ]; then
  mvn install -Dmaven.test.skip.exec=true
fi

mkdir -p $DOWNLOAD_CACHE

ES_VERSION=$(xmlstarlet sel -t -m "/_:project/_:properties/_:elasticsearch.version" -v . pom.xml)

echo "ES version: $ES_VERSION"

SG_SNAPSHOT=$(echo $REPO_DIR/plugin/target/releases/search-guard-flx-elasticsearch-plugin-*SNAPSHOT-es-$ES_VERSION.zip)

echo "Search Guard Snapshot: $SG_SNAPSHOT"

if [[ "$OSTYPE"  == "linux"* ]]; then
  ES_ARCHIVE="elasticsearch-$ES_VERSION-linux-x86_64.tar.gz"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  ES_ARCHIVE="elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz"
else
  echo "OS type $OSTYPE not supported"
  exit
fi

if [ ! -f "$DOWNLOAD_CACHE/$ES_ARCHIVE" ]; then
	wget "https://artifacts.elastic.co/downloads/elasticsearch/$ES_ARCHIVE" -P $DOWNLOAD_CACHE
fi

if [ -d "$INSTALL_DIR" ]; then
   rm -r "$INSTALL_DIR"
fi
   
mkdir -p "$INSTALL_DIR"

echo "Extracting $ES_ARCHIVE to $INSTALL_DIR"

tar xfz "$DOWNLOAD_CACHE/$ES_ARCHIVE" -C "$INSTALL_DIR" --strip-components 1

cd "$INSTALL_DIR"

echo "-Xms1g" >>config/jvm.options
echo "-Xmx1g" >>config/jvm.options

bin/elasticsearch-plugin install -v -b file:///$SG_SNAPSHOT

chmod +x "$INSTALL_DIR/plugins/search-guard-flx/tools/install_demo_configuration.sh"
"$INSTALL_DIR/plugins/search-guard-flx/tools/install_demo_configuration.sh" -y -i
  
echo "Starting ES"

bin/elasticsearch &
PID=$!

while ! nc -z localhost 9200; do
  sleep 2
done

until curl -k -Ss --fail -u "admin:admin" "https://localhost:9200/_cluster/health?wait_for_status=green&timeout=50s"; do
sleep 5
done

sleep 10
chmod +x "$INSTALL_DIR/plugins/search-guard-flx/tools/sgctl.sh"
"$INSTALL_DIR/plugins/search-guard-flx/tools/sgctl.sh" get-config -o "$INSTALL_DIR" --debug -v
RET=$?

kill -9 $PID

echo "RET $RET"
exit $RET