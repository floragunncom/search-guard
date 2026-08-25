#!/bin/bash

set -e

REPOSITORY=$1
ARTIFACT=$2
VERSION=$3
TYPE=$4
FILTER=$5

MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://maven.search-guard.com}"
GROUP_PATH="com/floragunn"
EXT="${TYPE#.}"

VERSION_URL="$MAVEN_BASE_URL/$REPOSITORY/$GROUP_PATH/$ARTIFACT/$VERSION"

case "$VERSION" in
  *-SNAPSHOT)
    # Snapshot deployments are timestamped; maven-metadata.xml maps
    # extension (+classifier) -> the concrete timestamped version string.
    METADATA=$(curl -Ss --fail "$VERSION_URL/maven-metadata.xml")

    FILE=$(printf '%s' "$METADATA" | awk -v artifact="$ARTIFACT" -v ext="$EXT" -v filter="$FILTER" '
      BEGIN { RS = "<" }
      function val(r) { sub(/^[^>]*>/, "", r); sub(/[[:space:]]+$/, "", r); return r }
      /^snapshotVersion>/   { classifier = ""; extension = ""; value = "" }
      /^classifier>/        { classifier = val($0) }
      /^extension>/         { extension  = val($0) }
      /^value>/             { value      = val($0) }
      /^\/snapshotVersion>/ {
        if (extension != ext || value == "") next
        file = artifact "-" value (classifier == "" ? "" : "-" classifier) "." extension
        if (filter != "" && substr(file, length(file) - length(filter) + 1) == filter) next
        if (file > latest) latest = file
      }
      END { print latest }
    ')
    ;;
  *)
    FILE="$ARTIFACT-$VERSION.$EXT"
    ;;
esac

if [ -z "$FILE" ]; then
  exit 1
fi

ARTIFACT_URL="$VERSION_URL/$FILE"

# Fail if the file is gone/expired.
curl -Ss --fail -o /dev/null --head "$ARTIFACT_URL"

echo "$ARTIFACT_URL"
