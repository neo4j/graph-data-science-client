# run `tox -e jupyter-notebook-session-ci`

import logging
import os
import random as rd
import signal
import sys
from types import FrameType
from typing import Optional

from aura_api_ci import AuraApiCI

logging.basicConfig(level=logging.DEBUG)


def main() -> None:
    client_id = os.environ["AURA_API_CLIENT_ID"]
    client_secret = os.environ["AURA_API_CLIENT_SECRET"]
    project_id = os.environ.get("AURA_API_TENANT_ID")
    aura_api = AuraApiCI(client_id=client_id, client_secret=client_secret, project_id=project_id)

    MAX_INT = 1000000
    instance_name = f"ci-build-{sys.argv[1]}" if len(sys.argv) > 1 else "ci-instance-" + str(rd.randint(0, MAX_INT))

    create_result = aura_api.create_instance(instance_name, memory="1GB", type="professional-db")
    instance_id = create_result["id"]
    logging.info(f"Creation of database with id '{instance_id}'")

    # Teardown instance on SIGNAL
    def handle_signal(sig: int, frame: Optional[FrameType]) -> None:
        logging.info("Received SIGNAL, tearing down instance")
        aura_api.teardown_instance(instance_id)
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        aura_api.check_running(instance_id)
        logging.info("Database up and running")

        uri = create_result["connection_url"]
        username = create_result["username"]
        password = create_result["password"]

        cmd = f"CLIENT_ID={client_id} CLIENT_SECRET={client_secret} PROJECT_ID={project_id} NEO4J_URI={uri} NEO4J_USERNAME={username} NEO4J_PASSWORD={password} tox -e jupyter-notebook-session-ci"

        if os.system(cmd) != 0:
            raise Exception("Failed to run notebooks")

    finally:
        aura_api.teardown_instance(instance_id)
        logging.info("Teardown of instance %s successful", instance_id)


if __name__ == "__main__":
    main()
