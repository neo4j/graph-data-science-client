style skip_notebooks="false":
     SKIP_NOTEBOOKS={{skip_notebooks}} ./scripts/makestyle && ./scripts/checkstyle

check-notebooks:
    ./scripts/nb2doc/check.sh

convert-notebooks:
    ./scripts/nb2doc/convert.sh

manual-docs:
    ./scripts/render_docs.sh

api-docs:
   ./scripts/render_api_docs

update-env:
    uv pip install --group dev -e .[networkx,ogb,rust-ext] --upgrade

pre-release:
    ./scripts/release_helper/pre_release.py

post-release-main version="":
    ./scripts/release_helper/post_release_main.py {{version}}

unit-tests extra_options="":
    pytest tests/unit {{extra_options}}

# just it test true "--durations=20"
it filter="" enterprise="true" extra_options="":
    #!/usr/bin/env bash
    set -e
    if [ "{{enterprise}}" = "true" ]; then
        ENV_DIR="scripts/test_envs/gds_plugin_enterprise"
        EXTRA_FLAGS="--include-model-store-location --include-enterprise {{extra_options}}"
    else
        ENV_DIR="scripts/test_envs/gds_plugin_community"
        EXTRA_FLAGS="{{extra_options}}"
    fi
    trap "cd $ENV_DIR && docker compose down" EXIT
    cd $ENV_DIR && docker compose up -d
    cd -
    pytest tests/integration $EXTRA_FLAGS --basetemp=tmp/ {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}


# such as `just it-v2 wcc`
it-v2 filter="" extra_options="":
    pytest tests/integrationV2 --include-integration-v2 --basetemp=tmp/ {{extra_options}} {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}


# runs the
session-v1-it:
    #!/usr/bin/env bash
    set -e
    ENV_DIR="scripts/test_envs/gds_session"
    trap "cd $ENV_DIR && docker compose down" EXIT
    cd $ENV_DIR && docker compose up -d
    cd -
    NEO4J_URI=bolt://localhost:7688 \
    NEO4J_USER=neo4j \
    NEO4J_PASSWORD=password \
    NEO4J_DB=neo4j \
    NEO4J_AURA_DB_URI=bolt://localhost:7687 \
    pytest tests --include-cloud-architecture


update-session-image:
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:latest

update-neo4j-image:
    docker pull neo4j:enterprise

update neo4j-aura-image:
    docker pull europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura-dev/neo4j-enterprise

prs:
    gh pr list --author "@me"
