#!/bin/bash

set -e

PACKAGE=com.floragunn.searchguard

if [[ "$1" == "privs" ]]; then
	PACKAGE="com.floragunn.searchguard.privileges"
elif [[ "$1" == "auth" ]]; then	
	PACKAGE="com.floragunn.searchguard.auth"
elif [[ $1 =~ [a-z]+ ]]; then	
	PACKAGE="com.floragunn.searchguard.$1"
else
    PACKAGE=$1
fi

echo "Setting log level for $PACKAGE to ${2:-DEBUG}"     	

curl --insecure -u admin:admin -X PUT "https://localhost:9200/_cluster/settings" -H "Content-Type: application/json" -d "{ \"transient\": { \"logger.$PACKAGE\": \"${2:-DEBUG}\" } }"

echo