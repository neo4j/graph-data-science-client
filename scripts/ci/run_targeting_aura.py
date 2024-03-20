import logging
import os
import random as rd
import sys
from time import sleep
from typing import Any, Dict

import requests as req

logging.basicConfig(level=logging.INFO)

CLIENT_ID = os.environ["AURA_API_CLIENT_ID"]
CLIENT_SECRET = os.environ["AURA_API_CLIENT_SECRET"]


def get_access_token() -> str:
    data = {
        "grant_type": "client_credentials",
    }

    # getting a token like {'access_token':'X','expires_in':3600,'token_type':'bearer'}
    response = req.post("https://api-staging.neo4j.io/oauth/token", data=data, auth=(CLIENT_ID, CLIENT_SECRET))

    response.raise_for_status()

    return response.json()["access_token"]  # type: ignore


def create_instance(name: str, access_token: str) -> Dict[str, Any]:
    CREATE_OK_MAX_WAIT_TIME = 10

    data = {
        "name": name,
        "memory": "8GB",
        "version": "5",
        "region": "europe-west1",
        "type": "gds",
        "cloud_provider": "gcp",
        "tenant_id": get_tenant_id(access_token),
    }

    should_retry = True
    wait_time = 1

    while should_retry:
        sleep(wait_time)
        wait_time *= 2

        response = req.post(
            "https://api-staging.neo4j.io/v1/instances",
            json=data,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        should_retry = response.status_code in [500, 502, 503, 504, 405] and CREATE_OK_MAX_WAIT_TIME > wait_time

        if should_retry:
            logging.debug(f"Error code: {response.status_code} - Retrying in {wait_time} s")

    response.raise_for_status()

    return response.json()["data"]  # type: ignore


def check_running(access_token: str, db_id: str) -> None:
    RUNNING_MAX_WAIT_TIME = 60 * 5

    should_retry = True
    wait_time = 1

    while should_retry:
        sleep(wait_time)
        wait_time *= 2

        response = req.get(
            f"https://api-staging.neo4j.io/v1/instances/{db_id}",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        instance_status = "?"
        if response.status_code == 200:
            instance_status = response.json()["data"]["status"]

        should_retry = (
            response.status_code in [500, 502, 503, 504] or instance_status == "creating"
        ) and RUNNING_MAX_WAIT_TIME > wait_time

        if should_retry:
            logging.debug(f"Status code: {response.status_code}, Status: {instance_status} - Retrying in {wait_time} s")

    response.raise_for_status()


def get_tenant_id(access_token: str) -> str:
    response = req.get(
        "https://api-staging.neo4j.io/v1/tenants",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    response.raise_for_status()

    raw_data = response.json()["data"]
    assert len(raw_data) == 1

    return raw_data[0]["id"]  # type: ignore


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


def teardown_instance(access_token: str, db_id: str) -> None:
    TEARDOWN_MAX_WAIT_TIME = 10

    should_retry = True
    wait_time = 1

    while should_retry:
        sleep(wait_time)
        wait_time *= 2

        response = req.delete(
            f"https://api-staging.neo4j.io/v1/instances/{db_id}",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        if response.status_code == 202:
            should_retry = False

        should_retry = (response.status_code in [500, 502, 503, 504]) and TEARDOWN_MAX_WAIT_TIME > wait_time

        if should_retry:
            logging.debug(f"Status code: {response.status_code} - Retrying in {wait_time} s")

    response.raise_for_status()


def main() -> None:
    access_token = get_access_token()
    logging.info("Access token for creation acquired")

    MAX_INT = 1000000
    instance_name = f"ci-build-{sys.argv[2]}" if len(sys.argv) > 1 else "ci-instance-" + str(rd.randint(0, MAX_INT))

    create_result = create_instance(instance_name, access_token)
    instance_id = create_result["id"]
    logging.info("Creation of database accepted")

    try:
        check_running(access_token, instance_id)
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
        access_token = get_access_token()
        logging.info("Access token for teardown acquired")

        teardown_instance(access_token, instance_id)
        logging.info("Teardown of instance %s successful", instance_id)


if __name__ == "__main__":
    main()
