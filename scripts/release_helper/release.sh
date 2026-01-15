#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

# Build artifacts
python3 -m build

# Deploy to PyPI
python3 -m twine upload dist/*