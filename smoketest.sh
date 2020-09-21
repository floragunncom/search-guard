#!/bin/bash
killall -9 java
shopt -s extglob
set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

ES_VERSION=7.9.1

rm -rf elasticsearch-$ES_VERSION
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz
if [ "$CI" == "true" ]; then
    chmod 777 elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz
fi
tar -xzf elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz
rm -rf elasticsearch-$ES_VERSION-darwin-x86_64.tar.gz

if [ "$CI" == "true" ]; then
    mvn clean package -DskipTests -s settings.xml
else
    mvn clean package -DskipTests
fi

PLUGIN_FILE=($DIR/plugin/target/releases/search-guard-suite!(*sgadmin*).zip)
URL=file://$PLUGIN_FILE
echo $URL
elasticsearch-$ES_VERSION/bin/elasticsearch-plugin install -b $URL
RET=$?

if [ $RET -eq 0 ]; then
    echo Installation ok
else
    echo Installation failed
    exit -1
fi

if [ "$CI" == "true" ]; then
  echo "Adding esuser user"
  useradd esuser
  mkdir /home/esuser
  chown esuser:esuser /home/esuser -R
  chown esuser:esuser $DIR/elasticsearch-$ES_VERSION -R
  usermod -aG sudo esuser
fi

echo "Plugin installation"

chmod +x elasticsearch-$ES_VERSION/plugins/search-guard-7/tools/install_demo_configuration.sh
./elasticsearch-$ES_VERSION/plugins/search-guard-7/tools/install_demo_configuration.sh -y -i -c
#ml does not work on cci anymore since 7.2 due to something related to https://github.com/elastic/elasticsearch/issues/41867
echo "xpack.ml.enabled: false" >> ./elasticsearch-$ES_VERSION/config/elasticsearch.yml
echo "ES starting up"
if [ "$CI" == "true" ]; then
    sudo -E -u esuser elasticsearch-$ES_VERSION/bin/elasticsearch -p es-smoketest-pid &
else
    elasticsearch-$ES_VERSION/bin/elasticsearch -p es-smoketest-pid &
fi

while ! nc -z 127.0.0.1 9200; do
  sleep 0.1 # wait for 1/10 of the second before check again
done

sleep 10

RES="$(curl -Ss --insecure -XGET -u admin:admin 'https://127.0.0.1:9200/_searchguard/authinfo' -H'Content-Type: application/json' | grep roles)"

if [ -z "$RES" ]; then
  echo "failed"
  kill -SIGTERM $(cat elasticsearch-$ES_VERSION/es-smoketest-pid)
  exit -1
else
  echo "$RES"
  echo ok
fi

./sgadmin_demo.sh
kill -SIGTERM $(cat elasticsearch-$ES_VERSION/es-smoketest-pid)
