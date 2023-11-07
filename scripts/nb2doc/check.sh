#!/bin/bash

set -o errexit
# set -o nounset
set -o pipefail
set -o xtrace

${PWD}/scripts/nb2doc/convert.sh

echo $GIT_DIR
export GIT_DIR=${PWD}/.git
echo $GIT_DIR

echo $GIT_INDEX_FILE
export GIT_INDEX_FILE=${PWD}/.git/index
echo $GIT_INDEX_FILE

if ! git diff --quiet ${PWD}/doc/modules/ROOT/pages/tutorials/
then
  echo "Please run /scripts/nb2doc/convert.sh to update docs"
  exit 1
fi


