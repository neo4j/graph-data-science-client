import logging
import os
import random as rd
import sys

from aura_api_ci import AuraApiCI

logging.basicConfig(level=logging.INFO)


def run_tests(uri: str, username: str, password: str) -> None:
    cmd = (
        f"NEO4J_URI={uri} NEO4J_USER={username} NEO4J_PASSWORD={password}"
        " tox -e $(tox -l | grep aura | grep main | paste -sd ',' -)"
    )

    if os.system(cmd) != 0:
        raise Exception("Failed to run tests")


def run_notebooks(uri: str, username: str, password: str) -> None:
    cmd = f"NEO4J_URI={uri} NEO4J_USER={username} NEO4J_PASSWORD={password} tox -e jupyter-notebook-ci"

    if os.system(cmd) != 0:
        raise Exception("Failed to run notebooks")


def main() -> None:
    aura_api = AuraApiCI()

    MAX_INT = 1000000
    instance_name = f"ci-build-{sys.argv[2]}" if len(sys.argv) > 1 else "ci-instance-" + str(rd.randint(0, MAX_INT))

    create_result = aura_api.create_ds_instance(instance_name)
    instance_id = create_result["id"]
    logging.info("Creation of database accepted")

    try:
        aura_api.check_running(instance_id)
        logging.info("Database %s up and running", instance_id)

        if sys.argv[1] == "tests":
            run_tests(
                create_result["connection_url"],
                create_result["username"],
                create_result["password"],
            )
            logging.info("Tests ran successfully")
        elif sys.argv[1] == "notebooks":
            run_notebooks(
                create_result["connection_url"],
                create_result["username"],
                create_result["password"],
            )
            logging.info("Notebooks ran successfully")
        else:
            logging.error(f"Invalid target: {sys.argv[1]}")
    finally:
        aura_api.teardown_instance(instance_id)
        logging.info("Teardown of instance %s successful", instance_id)


if __name__ == "__main__":
    main()
