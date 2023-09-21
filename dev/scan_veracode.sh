#!/bin/bash

if [ -z "$VERACODE_API_KEY_ID" ];then
	echo "No VERACODE_API_KEY_ID set"
	exit 1
fi

if [ -z "$VERACODE_API_KEY_SECRET" ];then
	echo "No VERACODE_API_KEY_SECRET set"
	exit 1
fi

export APP_NAME="Search Guard Security Suite 7"
export SANDBOX_NAME="cipipeline-master"
export APP_VERSION="master-$CI_PIPELINE_ID-$(date)"

set -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR/.."

echo "Build SG with veracode flavour..."
mvn clean package -Pveracode,dlic,enterprise -DskipTests #> /dev/null 2>&1
SCAN_FILE_DIR="$DIR/../plugin/target/releases"
SCAN_FILE="$SCAN_FILE_DIR/veracode_upload.zip"

FILESIZE=$(wc -c <"$SCAN_FILE")
echo ""
echo "Upload $SCAN_FILE with a size of $((FILESIZE / 1048576)) mb"

#https://docs.veracode.com/r/r_uploadandscan
docker run --rm \
    --env VERACODE_API_KEY_ID=$VERACODE_API_KEY_ID \
    --env VERACODE_API_KEY_SECRET=$VERACODE_API_KEY_SECRET \
    -v "$SCAN_FILE_DIR/:/myapp/" \
    veracode/api-wrapper-java:cmd \
        -action UploadAndScan \
        -sandboxname "$SANDBOX_NAME" \
        -appname "$APP_NAME" \
        -version "$APP_VERSION" \
        -maxretrycount 3 \
        -createprofile "false" \
        -scantimeout 60 \
        -scanpollinginterval 60 \
        -toplevel true \
        -scanallnonfataltoplevelmodules false \
        -include "*search-guard*,*codova*,*fluent-collections*" \
        -exclude "*listen*avoid-conflict-with*,bcpkix*" \
        -deleteincompletescan 2 \
        -filepath /myapp/veracode_upload.zip
