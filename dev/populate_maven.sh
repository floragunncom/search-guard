#!/bin/bash

echo "Populate maven script"
echo "Value of MAVEN_OPTS = $MAVEN_OPTS"
echo "Value of MAVEN_CLI_OPTS = $MAVEN_CLI_OPTS"

set -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GROUP_ID="org.elasticsearch.plugin"
VERSION="$1"
[ -z "$VERSION" ] && (echo "Usage: populate_maven.sh <elasticsearch version> [remote] [download-dir]";exit 1;)
TARGET="${2:-local}"
DIR="${3:-$SCRIPT_DIR/.downloads}"
echo "Installing artifacts for Elasticsearch $VERSION into $TARGET repo from $DIR"
ARCH="darwin-aarch64" #arch does not matter, because we are only interested in .jar files



mkdir -p "$DIR"

if [ ! -f "$DIR/elasticsearch-${VERSION}-${ARCH}.tar.gz" ]; then
  curl https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${VERSION}-${ARCH}.tar.gz -o "$DIR/elasticsearch-${VERSION}-${ARCH}.tar.gz"
fi

tar -xzf "$DIR/elasticsearch-${VERSION}-${ARCH}.tar.gz" --directory "$DIR/"

PATH_PREFIX="$DIR/elasticsearch-${VERSION}/modules"
echo "Value of variable PATH_PREFIX = $PATH_PREFIX"

modules=(rank-eval lang-mustache lang-painless reindex parent-join percolator rest-root)

for module in "${modules[@]}"
do

cat <<EOT > ".tmp.$module-$VERSION.pom"
<?xml version="1.0" encoding="UTF-8"?>
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.elasticsearch.plugin</groupId>
  <artifactId>$module</artifactId>
  <version>$VERSION</version>
  <description>POM was created from install:install-file</description>
  <licenses>
    <license>
      <name>Elastic License 2.0 (ELv2)</name>
      <url>https://raw.githubusercontent.com/elastic/elasticsearch/master/licenses/ELASTIC-LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
    <license>
        <name>Server Side Public License (SSPL) version 1</name>
        <url>https://www.mongodb.com/licensing/server-side-public-license</url>
        <distribution>repo</distribution>
    </license>
  </licenses>
</project>
EOT



  if [[ $TARGET != "remote" ]];then
      mvn org.apache.maven.plugins:maven-install-plugin:3.0.0-M1:install-file -Dfile="$PATH_PREFIX/$module/$module-$VERSION.jar" -DpomFile=".tmp.$module-$VERSION.pom"
  else
      mvn -s "$SCRIPT_DIR/../settings.xml" org.apache.maven.plugins:maven-deploy-plugin:3.0.0-M2:deploy-file -Dfile="$PATH_PREFIX/$module/$module-$VERSION.jar" -DpomFile=".tmp.$module-$VERSION.pom" -DrepositoryId="third-party" -Durl="https://maven.search-guard.com:443/third-party"
  fi
done

cat <<EOT > ".tmp.netty-$VERSION.pom"
<?xml version="1.0" encoding="UTF-8"?>
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <modelVersion>4.0.0</modelVersion>
  <groupId>org.elasticsearch.plugin</groupId>
  <artifactId>transport-netty4</artifactId>
  <version>$VERSION</version>
  <description>POM was created from install:install-file</description>
  <licenses>
    <license>
      <name>Elastic License 2.0 (ELv2)</name>
      <url>https://raw.githubusercontent.com/elastic/elasticsearch/master/licenses/ELASTIC-LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
    <license>
        <name>Server Side Public License (SSPL) version 1</name>
        <url>https://www.mongodb.com/licensing/server-side-public-license</url>
        <distribution>repo</distribution>
    </license>
  </licenses>
</project>
EOT

if [[ $TARGET != "remote" ]];then
    mvn org.apache.maven.plugins:maven-install-plugin:3.0.0-M1:install-file -Dfile="$PATH_PREFIX/transport-netty4/transport-netty4-$VERSION.jar" -DpomFile=".tmp.netty-$VERSION.pom"
else
    mvn -s "$SCRIPT_DIR/../settings.xml" org.apache.maven.plugins:maven-deploy-plugin:3.0.0-M2:deploy-file -Dfile="$PATH_PREFIX/transport-netty4/transport-netty4-$VERSION.jar" -DpomFile=".tmp.netty-$VERSION.pom" -DrepositoryId="third-party" -Durl="https://maven.search-guard.com:443/third-party"
fi

