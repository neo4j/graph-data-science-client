#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

# TODO: remove
echo $(ls -lad .git)
echo $(which git)

${PWD}/scripts/nb2doc/convert.sh

if ! $(which git) diff --quiet doc/modules/ROOT/pages/tutorials/;
  then
    echo "Please run /scripts/nb2doc/convert.sh to update docs"
    exit 1
fi


