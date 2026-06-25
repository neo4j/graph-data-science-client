style skip_notebooks="false":
     uv sync --frozen
     SKIP_NOTEBOOKS={{skip_notebooks}} ./scripts/makestyle && ./scripts/checkstyle

# Comprehensive CI-style check: Python style, notebook docs, and Ruby style
checkstyle-all:
    #!/usr/bin/env bash
    set -e
    uv sync --group dev-base
    # Python style
    ./scripts/checkstyle
    ./scripts/nb2doc/check.sh
    # Ruby style
    cd doc/tests
    bundle install
    bundle exec rubocop

check-notebooks:
    ./scripts/nb2doc/check.sh

convert-notebooks:
    ./scripts/nb2doc/convert.sh

manual-docs:
    ./scripts/render_docs.sh

api-docs:
   ./scripts/render_api_docs

pre-release:
    ./scripts/release_helper/pre_release.py

post-release-main version="":
    ./scripts/release_helper/post_release_main.py {{version}}

unit-tests extra_options="":
    uv run --group test pytest tests/unit {{extra_options}}

# such as `just it wcc`
it filter="" extra_options="":
    uv run --group test pytest tests/integration --basetemp=tmp/ {{extra_options}} {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}

test-session-notebooks:
    #!/usr/bin/env bash
    # expects Aura API credentials to be set as env vars
    set -uo pipefail
    rc=0
    uv run scripts/ci/run_session_notebooks.py              || rc=1
    uv run scripts/ci/run_session_notebooks_self_managed.py || rc=1
    exit $rc

test-aurads-notebooks:
    # expects Aura API credentials to be set as env vars
    uv run scripts/ci/run_plugin_notebooks_aura.py

# Run the plugin notebooks against a local Neo4j with the GDS plugin (AuraDS-like).
# `filter` selects notebooks by name substring (e.g. `hashgnn`); empty runs all.
# `enterprise=true` requires a license at ${HOME}/.gds_license; `enterprise=false` uses community.
test-plugin-notebooks-local filter="" enterprise="true":
    #!/usr/bin/env bash
    set -e
    if [ "{{enterprise}}" = "true" ]; then
        ENV_DIR="scripts/test_envs/gds_plugin_enterprise"
        if [ ! -f "${HOME}/.gds_license" ]; then
            echo "Error: GDS enterprise license file not found at ${HOME}/.gds_license"
            exit 1
        fi
    else
        ENV_DIR="scripts/test_envs/gds_plugin_community"
    fi
    trap "cd $ENV_DIR && docker compose down" EXIT
    cd $ENV_DIR && docker compose up -d
    cd -
    echo "Waiting for Neo4j to be ready on http://localhost:7474 ..."
    for i in $(seq 1 90); do
        if curl -sf http://localhost:7474 > /dev/null 2>&1; then
            echo "Neo4j is up"
            break
        fi
        if [ "$i" = "90" ]; then
            echo "Error: Neo4j did not become ready in time"
            exit 1
        fi
        sleep 2
    done
    # The compose env runs with NEO4J_AUTH=none, matching the notebook defaults
    # (bolt://localhost:7687, user "neo4j", empty password).
    uv run --group notebook-ci ./scripts/run_notebooks.py {{filter}}

test-tox-partition number-of-partitions partition-index:
    uv run --group test scripts/ci/run_tox_environments.py {{number-of-partitions}} {{partition-index}}


update-aga-images:
    # docker pull is incremental: it only downloads layers when the remote digest is new,
    # otherwise it reports "Image is up to date".
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:aura-release
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/mock-runtime-api:latest
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/python-runtime:latest

update-neo4j-image:
    docker pull neo4j:enterprise

update-neo4j-aura-image:
    # check https://console.cloud.google.com/artifacts/docker/neo4j-aura-image-artifacts/europe-west1/aura-dev/neo4j-enterprise?project=neo4j-aura-image-artifacts to pull a later image
    docker pull europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura-dev/neo4j-enterprise:2026.03.1

update-test-images:
    just update-aga-images
    just update-neo4j-image
    just update-neo4j-aura-image

doc-tests enterprise="true":
    #!/usr/bin/env bash
    set -e
    if [ "{{enterprise}}" = "true" ]; then
        ENV_DIR="scripts/test_envs/gds_plugin_enterprise"
        if [ ! -f "${HOME}/.gds_license" ]; then
            echo "Error: GDS enterprise license file not found at ${HOME}/.gds_license"
            exit 1
        fi
    else
        ENV_DIR="scripts/test_envs/gds_plugin_community"
    fi
    trap "cd $ENV_DIR && docker compose down" EXIT
    cd $ENV_DIR && docker compose up -d
    cd -
    PYTHON=$(uv run which python)
    cd doc/tests && bundle install && bundle exec ruby test_docs.rb $PYTHON {{ if enterprise != "true" { "-n test_community" } else { "" } }}

prs:
    gh pr list --author "@me"
