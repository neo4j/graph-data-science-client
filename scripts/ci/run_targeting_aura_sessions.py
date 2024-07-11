# run `tox -e jupyter-notebook-session-ci`

import logging
import os
import random as rd
import sys

from aura_api_ci import AuraApiCI

logging.basicConfig(level=logging.INFO)


def main() -> None:
    client_id = os.environ["AURA_API_CLIENT_ID"]
    client_secret = os.environ["AURA_API_CLIENT_SECRET"]
    tenant_id = os.environ.get("TENANT_ID")
    aura_api = AuraApiCI(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

    MAX_INT = 1000000
    instance_name = f"ci-build-{sys.argv[1]}" if len(sys.argv) > 1 else "ci-instance-" + str(rd.randint(0, MAX_INT))

    create_result = aura_api.create_instance(instance_name, memory="1GB", type="professional-db")
    instance_id = create_result["id"]
    logging.info("Creation of database accepted")

    try:
        aura_api.check_running(instance_id)
        logging.info("Database %s up and running", instance_id)

        uri = create_result["connection_url"]
        username = create_result["username"]
        password = create_result["password"]

        cmd = f"AURA_DB_ADDRESS={uri} AURA_DB_USER={username} AURA_DB_PW={password} tox -e jupyter-notebook-session-ci"

        if os.system(cmd) != 0:
            raise Exception("Failed to run notebooks")

    finally:
        aura_api.teardown_instance(instance_id)
        logging.info("Teardown of instance %s successful", instance_id)


if __name__ == "__main__":
    main()
