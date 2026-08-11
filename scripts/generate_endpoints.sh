#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

cd "$(dirname "$0")"

uv run python -m endpoint_codegen.generate
uvx ty check --fix ../src/graphdatascience/procedure_surface/api/embedding
uvx ty check --fix ../src/graphdatascience/procedure_surface/arrow/embedding
uvx ruff check --fix ../src/graphdatascience/procedure_surface/
uv run ruff format ../src/graphdatascience/procedure_surface/api/embedding ../src/graphdatascience/procedure_surface/arrow/embedding
