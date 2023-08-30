#!/bin/bash
DOCKER_USER=$1
DOCKER_REPO=${2:-docker.io}
COMPONENT=$3
COMPONENT_VERSION=$4
BUILD_ARGS=$5
PLATFORMS=${6:-linux/arm64,linux/amd64}
CHECK_TAG=${7:-true}

retVal=0

declare -A params

params=(
    ["DOCKER_USER"]=$DOCKER_USER
    ["COMPONENT"]=$COMPONENT
    ["COMPONENT_VERSION"]=$COMPONENT_VERSION
    ["BUILD_ARGS"]=$BUILD_ARGS
)

for param in "${!params[@]}"; do
    if [ -z "${params[$param]}" ]; then
        echo "Error: $param is not set."
        exit 1
    fi
done

dependencies=("docker" "curl")

for cmd in "${dependencies[@]}"; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "$cmd could not be found"
        exit 1
    fi
done


check() {
    local status=$?
    if [ $status -ne 0 ]; then
         echo "ERR - The command $1 failed with status $status"
         retVal=$status
    fi
}

check_tag() {
  local docker_registry=$1
  local tag=$2
  if [ "$DOCKER_REPO" == "docker.io" ]; then
    local docker_repo="hub.docker.com"
  else
    local docker_repo=$DOCKER_REPO  
  fi
  
  response_code=$(curl -s -o /dev/null -w "%{http_code}" https://$docker_repo/v2/repositories/$docker_registry/tags/$tag || true)

  if [ "$response_code" == "200" ]; then
      echo "Error: The image $docker_registry:$tag already exists on $DOCKER_REPO" >&2
      exit 1
  fi
}

build() {
    local component="$1"
    local tag="$DOCKER_REPO/$DOCKER_USER/$COMPONENT:$2"
    echo "Build and push image $tag for $PLATFORMS"
    docker buildx build --push  --platform "$PLATFORMS" -t "$tag" "${@:3}" .      
    check "Buildx $tag"
}

export DOCKER_SCAN_SUGGEST=false
export BUILDKIT_PROGRESS=plain

docker context create tls-environment
docker buildx create --name esxbuilder  --use tls-environment || true
docker buildx use esxbuilder
docker buildx inspect --bootstrap

if [ "$CHECK_TAG" == "true" ]; then
  check_tag $DOCKER_USER/$COMPONENT $COMPONENT_VERSION
fi 

build $COMPONENT "$COMPONENT_VERSION" $BUILD_ARGS

if [ $retVal -eq 0 ]; then
  echo "Finished with success"
else
  echo "Finished with errors: $retVal"
fi

exit $retVal