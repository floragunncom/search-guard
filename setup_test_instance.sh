#!/bin/bash

set -e

MAIN_DIR=$(echo ~/searchguard-test/)
DOWNLOAD_CACHE="$MAIN_DIR/download-cache"
INSTALL_DIR="$MAIN_DIR/es"
REPO_DIR=$(pwd)

mvn install -Dmaven.test.skip.exec=true

SG_SNAPSHOT=$(echo $REPO_DIR/plugin/target/releases/search-guard-suite-plugin-*.zip)

echo "Search Guard Suite Snapshot: $SG_SNAPSHOT" 

mkdir -p $DOWNLOAD_CACHE

ES_VERSION=$(xmlstarlet sel -t -m "/_:project/_:properties/_:elasticsearch.version" -v . pom.xml)

echo "ES version: $ES_VERSION"

ES_ARCHIVE="elasticsearch-$ES_VERSION-linux-x86_64.tar.gz"

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

bin/elasticsearch-plugin install -b file:///$SG_SNAPSHOT

chmod u+x plugins/search-guard-7/tools/install_demo_configuration.sh

./plugins/search-guard-7/tools/install_demo_configuration.sh -i

echo "Starting ES"

bin/elasticsearch