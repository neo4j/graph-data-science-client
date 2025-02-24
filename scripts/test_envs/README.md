# Test environments

Each folder represents an environemnt to test the python client against.

## Requirements

* You should be able to run a compose.yaml file (such as through `docker compose`)
* For gds_plugin_enterprise, you need to modify the compose file to find a license under `$HOME/.gds_license`

## Workflow

1. Switch to the env folder you like to use -- `cd gds_plugin_community``
2. Start the environment using `docker compose up -d`
3. Run your tests such as `pytest graphdatascience/tests/integration` (best in a different shell)
4. Scale down environment using `docker compose kill` from the env folder
