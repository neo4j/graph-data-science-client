#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

${PWD}/scripts/nb2doc/convert.sh

# Without this, TeamCity complains with a
# "warning: Not a git repository" error
export GIT_DIR=${PWD}/.git

if ! git diff --quiet ${PWD}/doc/modules/ROOT/pages/tutorials/
then
  diff_files=`git diff ${PWD}/doc/modules/ROOT/pages/tutorials/ | grep "diff --git" | cut -d " " -f3`
  echo "The following notebooks need to be updated in the docs"
  echo "${diff_files}"
  echo "Please run /scripts/nb2doc/convert.sh to update docs"
  exit 1
fi


