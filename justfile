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

rb-makestyle:
    #!/usr/bin/env bash
    # Ruby style
    cd doc/tests
    bundle install
    bundle exec rubocop -A

check-notebooks:
    ./scripts/nb2doc/check.sh

convert-notebooks:
    ./scripts/nb2doc/convert.sh

manual-docs:
    ./scripts/render_docs.sh

api-docs:
   ./scripts/render_api_docs

# Render both the manual and the API reference docs and serve them locally.
# Manual -> http://localhost:8000 ; API refdocs -> http://localhost:8001
render-docs:
    #!/usr/bin/env bash
    set -e
    # Build the API reference docs (Sphinx) and the manual (Antora).
    # Force the manual's API-reference links to the locally-served refdocs (only for this build).
    (cd doc/sphinx && uv run --group docs-ci make clean html)
    (cd doc && npm install && npm install @neo4j-antora/antora-page-roles --save && \
        npx antora preview.yml --attribute gds-api-uri=http://localhost:8001 --stacktrace --log-format=pretty)

    cleanup() {
        trap - INT TERM EXIT
        kill "${api_pid:-}" "${manual_pid:-}" 2>/dev/null || true
    }
    trap cleanup INT TERM EXIT

    (cd doc/sphinx/build/html && exec python3 -m http.server 8001) &
    api_pid=$!
    (cd doc && exec node server.js) &
    manual_pid=$!

    echo ""
    echo "Manual:      http://localhost:8000"
    echo "API refdocs: http://localhost:8001"
    echo "Press Ctrl-C to stop both."
    wait

pre-release:
    uv run --group dev-base scripts/release_helper/pre_release.py

post-release-main version="":
    uv run scripts/release_helper/post_release_main.py {{version}}

unit-tests extra_options="":
    uv run --group test pytest tests/unit {{extra_options}}

# such as `just it wcc`
it filter="" extra_options="":
    uv run --group test pytest tests/integration --durations=10 --basetemp=tmp/ {{extra_options}} {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}

# Same as `it`, but against the latest (master) session, plugin, and remote-ops images instead of
# the released ones. Use for endpoints that are on master but not yet released
it-master filter="" extra_options="":
    GDS_SESSION_IMAGE="europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:latest" \
    NEO4J_DATABASE_IMAGE="europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/neo4j-with-gds-plugin:latest" \
    NEO4J_AURA_DATABASE_IMAGE="europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/neo4j-with-gds-remote-ops:latest" \
    uv run --group test pytest tests/integration --durations=10 --basetemp=tmp/ {{extra_options}} {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}

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

test-tox-partition number-of-partitions partition-index: update-test-images
    uv run --group test scripts/ci/run_tox_environments.py {{number-of-partitions}} {{partition-index}}


update-aga-images:
    #!/usr/bin/env bash
    set -euo pipefail

    docker pull "${GDS_SESSION_IMAGE:-europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:aura-release}"
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/mock-runtime-api:latest
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/python-runtime:latest
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/mock-gds-api:latest

update-neo4j-image:
    docker pull "${NEO4J_DATABASE_IMAGE:-neo4j:enterprise}"

update-neo4j-aura-image:
    #!/usr/bin/env bash
    set -euo pipefail
    # Use NEO4J_AURA_DATABASE_IMAGE if set, else the version from latest_neo4j_version()
    # (the same logic the integration tests use in tests/integration/conftest.py).
    # check https://console.cloud.google.com/artifacts/docker/neo4j-aura-image-artifacts/europe-west1/aura-dev/neo4j-enterprise?project=neo4j-aura-image-artifacts to inspect available tags
    if [ -n "${NEO4J_AURA_DATABASE_IMAGE:-}" ]; then
        image="${NEO4J_AURA_DATABASE_IMAGE}"
    else
        version=$(uv run --group test python -c "from tests.integration.conftest import latest_neo4j_version; print(latest_neo4j_version())")
        image="europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura-dev/neo4j-enterprise:${version}"
    fi
    echo "Pulling ${image}"
    docker pull "${image}"

update-test-images:
    just update-aga-images
    just update-neo4j-image
    just update-neo4j-aura-image


test-docs-plugin:
    uv run --group dev scripts/ci/run_doc_tests_plugin.py

prs:
    gh pr list --author "@me"
