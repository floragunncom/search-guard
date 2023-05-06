#!/bin/bash
SCRIPT_PATH="${BASH_SOURCE[0]}"
if ! [ -x "$(command -v realpath)" ]; then
    if [ -L "$SCRIPT_PATH" ]; then
        
        [ -x "$(command -v readlink)" ] || { echo "Not able to resolve symlink. Install realpath or readlink." 1>&2;exit 1; }
        
        # try readlink (-f not needed because we know its a symlink)
        DIR="$( cd "$( dirname $(readlink "$SCRIPT_PATH") )" && pwd -P)"
    else
        DIR="$( cd "$( dirname "$SCRIPT_PATH" )" && pwd -P)"
    fi
else
    DIR="$( cd "$( dirname "$(realpath "$SCRIPT_PATH")" )" && pwd -P)"
fi

BIN_PATH="java"

if [ -z "$JAVA_HOME" ]; then
    echo "WARNING: JAVA_HOME not set, will use $(which $BIN_PATH)" 1>&2
else
    BIN_PATH="$JAVA_HOME/bin/java"
fi

echo "JAVA_HOME: $JAVA_HOME"
echo "BIN_PATH: $BIN_PATH"
find / -name java 2>/dev/null
"$BIN_PATH" --version

ls -la
pwd

set +e
md5sum resources/certificates/CN\=kirk\,OU\=client\,O\=client\,L\=Test\,C\=DE-keystore.jks
cat resources/certificates/CN\=kirk\,OU\=client\,O\=client\,L\=Test\,C\=DE-keystore.jks | base64
md5sum resources/certificates/truststore.jks
cat resources/certificates/truststore.jks | base64




"$BIN_PATH" $JAVA_OPTS -Dio.netty.tryReflectionSetAccessible=false -Dio.netty.noUnsafe=true -Dorg.apache.logging.log4j.simplelog.StatusLogger.level=OFF -cp "$DIR/../*:$DIR/../../../lib/*:$DIR/../deps/*" com.floragunn.searchguard.tools.SearchGuardAdmin "$@"

