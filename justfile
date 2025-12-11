style:
     ./scripts/makestyle && ./scripts/checkstyle

convert-notebooks:
    ./scripts/nb2doc/convert.sh

unit-tests:
    pytest tests/unit

it filter="" enterprise="true":
    #!/usr/bin/env bash
    set -e
    if [ "{{enterprise}}" = "true" ]; then
        ENV_DIR="scripts/test_envs/gds_plugin_enterprise"
        EXTRA_FLAGS="--include-model-store-location --include-enterprise"
    else
        ENV_DIR="scripts/test_envs/gds_plugin_community"
        EXTRA_FLAGS=""
    fi
    trap "cd $ENV_DIR && docker compose down" EXIT
    cd $ENV_DIR && docker compose up -d
    cd -
    pytest tests/integration $EXTRA_FLAGS --basetemp=tmp/ {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}


# such as `just it-v2 wcc`
it-v2 filter="":
    pytest tests/integrationV2 --include-integration-v2 --basetemp=tmp/ {{ if filter != "" { "-k '" + filter + "'" } else { "" } }}
