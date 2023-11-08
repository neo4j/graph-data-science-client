#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

${PWD}/scripts/nb2doc/convert.sh

# Without this, TeamCity complains with a
# "warning: Not a git repository" error
export GIT_DIR=${PWD}/.git

if ! git diff --quiet ${PWD}/doc/modules/ROOT/pages/tutorials/
then
  echo "Please run /scripts/nb2doc/convert.sh to update docs"
  exit 1
fi


