#!/usr/bin/env bash

set -e
set -o pipefail

if [[ -z "$GITHUB_TOKEN" ]]; then
  echo "Set the GITHUB_TOKEN env variable."
  exit 1
fi

RELEASE=${GITHUB_REF##*/}
body='{
  "tag_name": "'${RELEASE}'",
  "target_commitish": "master",
  "name": "'${RELEASE}'",
  "body": "Draft release for '${RELEASE}'",
  "draft": true,
  "prerelease": false
}'

curl -X POST -H "Authorization: token $GITHUB_TOKEN" \
  --data "$body" "https://api.github.com/repos/$GITHUB_REPOSITORY/releases"
