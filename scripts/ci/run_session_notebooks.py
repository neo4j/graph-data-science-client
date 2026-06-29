import logging
import os
import random as rd
import signal
import sys
from types import FrameType

from aura_api_ci import AuraApiCI

logging.basicConfig(level=logging.DEBUG)


def main() -> None:
    aura_api = AuraApiCI.from_env()
    client_id = aura_api.client_id
    client_secret = aura_api.client_secret
    project_id = aura_api.get_tenant_id()

    logging.info("Using project_id=%s", project_id)

    build_id = os.environ.get("BUILD_ID", None)
    instance_name = f"ci-build-{build_id}" if build_id else "ci-instance-" + str(rd.randint(0, 1000000))
    instance_type = "enterprise-db" if os.environ.get("ENTERPRISE", "true").lower() == "true" else "professional-db"
    create_result = aura_api.create_instance(instance_name, memory="2GB", type=instance_type)
    instance_id = create_result["id"]
    logging.info(f"Creation of database with id '{instance_id}'")

    # Teardown instance on SIGNAL
    def handle_signal(sig: int, frame: FrameType | None) -> None:
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
        instance_id = create_result["id"]

        cmd = f"AURA_ENV=staging CLIENT_ID={client_id} CLIENT_SECRET={client_secret} PROJECT_ID={project_id} AURA_INSTANCEID={instance_id} NEO4J_URI={uri} NEO4J_USERNAME={username} NEO4J_PASSWORD={password} uv run --group notebook-aura-ci ./scripts/run_notebooks.py sessions-attached"

        if os.system(cmd) != 0:
            raise Exception("Failed to run notebooks")

    finally:
        aura_api.teardown_instance(instance_id)
        logging.info("Teardown of instance %s successful", instance_id)


if __name__ == "__main__":
    main()
