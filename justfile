style skip_notebooks="false":
     SKIP_NOTEBOOKS={{skip_notebooks}} ./scripts/makestyle && ./scripts/checkstyle

convert-notebooks:
    ./scripts/nb2doc/convert.sh

unit-tests:
    pytest tests/unit

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


update-session:
    docker pull europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:latest
