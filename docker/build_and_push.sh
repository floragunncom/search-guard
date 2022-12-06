#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
set -e

DOCKER_REPO="$1"
DOCKER_USER="$2"
SG_FLAVOUR="$3"
ES_VERSION="$4"
SG_VERSION="$5"

# By default we build 64bit images for amd and arm
# The arm images can also run on Apple M1 chips and AWS Graviton
DEFAULT_PLATFORMS="linux/arm64,linux/amd64"

PREFIX=""
POSTFIX="-tmp"

echo "Build and push search-guard $SG_FLAVOUR image to $DOCKER_REPO/$DOCKER_USER for ES $ES_VERSION and SG $SG_VERSION"

export DOCKER_SCAN_SUGGEST=false
export BUILDKIT_PROGRESS=plain

retVal=0

check() {
    local status=$?
    if [ $status -ne 0 ]; then
         echo "ERR - The command $1 failed with status $status"
         retVal=$status
    fi
}

build() {
    TAG="$DOCKER_REPO/$DOCKER_USER/$PREFIX${1}$POSTFIX:$2"
    echo "Build and push image $TAG for $DEFAULT_PLATFORMS"

    docker buildx build -f "$DIR/Dockerfile" --push --platform "$DEFAULT_PLATFORMS" -t "$TAG" --target "$3" --build-arg ES_FLAVOUR="$4" --build-arg ES_VERSION="$ES_VERSION" --build-arg SG_VERSION="$SG_VERSION" .
    check "  Buildx $TAG"    
}

build_flx() {
    ES_FLAVOUR="$1"
    VERSION="$SG_VERSION-es-$ES_VERSION$ES_FLAVOUR"
    build "search-guard-flx" "$VERSION" "flx" "$ES_FLAVOUR"
}

build_non_flx() {
    ES_FLAVOUR="$1"
    VERSION="$SG_VERSION-es-$ES_VERSION$ES_FLAVOUR"
    build "search-guard" "$VERSION" "non-flx" "$ES_FLAVOUR"
}

docker context create tls-environment > /dev/null 2>&1 || true
docker buildx create --name sgxbuilder --use tls-environment > /dev/null 2>&1 || true
BUILDX_INSPECT=$(docker buildx inspect --bootstrap)
check "  buildx inspect" 

IFS=',' read -r -a DEFAULT_PLATFORMS_AR <<< "$DEFAULT_PLATFORMS"

for platform in "${DEFAULT_PLATFORMS_AR[@]}"
do
    if [[ $BUILDX_INSPECT != *" $platform"* ]];then
        echo "Platform $platform noch supported by buildx"
        exit 1
    fi
done


if [ "$SG_FLAVOUR" = "flx" ]; then
    build_flx ""
    if [ "$ES_VERSION" = "7.10.2" ]; then
        build_flx "-oss"
    fi
else
    build_non_flx ""
    if [ "$ES_VERSION" = "7.10.2" ]; then
        build_non_flx "-oss"
    fi
fi


if [ $retVal -eq 0 ]; then
  echo "Finished with success"
else
  echo "Finished with errors: $retVal"
fi

exit $retVal