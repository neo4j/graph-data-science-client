import logging
import random as rd
from time import sleep
from typing import Any, Dict

import requests as req

logging.basicConfig(level=logging.INFO)

CLIENT_ID = "duNi01VCb9IKBrZYgt3VZwb8tVPHQ3gm"
CLIENT_SECRET = "g3qjqkB5z5VcGIolJ9DG1Zw54TGEwuV60d_R4uS7kdqXGtkOlcnnPLaQEdYs7qEX"

DEVENV_URI = "https://api-devflorentin.neo4j-dev.io"
AURA_API_VERSION = "v1beta5"


def get_access_token() -> str:
    data = {
        "grant_type": "client_credentials",
    }

    # getting a token like {'access_token':'X','expires_in':3600,'token_type':'bearer'}
    response = req.post(f"{DEVENV_URI}/oauth/token", data=data, auth=(CLIENT_ID, CLIENT_SECRET))

    response.raise_for_status()

    return response.json()["access_token"]  # type: ignore


def create_auradb_pro(access_token: str) -> Dict[str, Any]:
    CREATE_OK_MAX_WAIT_TIME = 10
    MAX_INT = 2**31

    data = {
        "name": "ci-instance-" + str(rd.randint(0, MAX_INT)),
        "memory": "1GB",
        "version": "5",
        "region": "europe-west1",
        "type": "professional-db",
        "cloud_provider": "gcp",
        "tenant_id": get_tenant_id(access_token),
    }

    should_retry = True
    wait_time = 1

    while should_retry:
        sleep(wait_time)
        wait_time *= 2

        response = req.post(
            f"{DEVENV_URI}/{AURA_API_VERSION}/instances",
            json=data,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        should_retry = response.status_code in [500, 502, 503, 504, 405] and CREATE_OK_MAX_WAIT_TIME > wait_time

        if should_retry:
            logging.debug(f"Error code: {response.status_code} - Retrying in {wait_time} s")

    response.raise_for_status()

    return response.json()["data"]  # type: ignore


def get_tenant_id(access_token: str) -> str:
    response = req.get(
        f"{DEVENV_URI}/{AURA_API_VERSION}/tenants",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    print(response.text)
    response.raise_for_status()

    raw_data = response.json()["data"]
    assert len(raw_data) == 1

    return raw_data[0]["id"]  # type: ignore


def check_running(access_token: str, db_id: str) -> None:
    RUNNING_MAX_WAIT_TIME = 60 * 5

    should_retry = True
    wait_time = 1

    while should_retry:
        sleep(wait_time)
        wait_time *= 2

        response = req.get(
            f"{DEVENV_URI}/{AURA_API_VERSION}/instances/{db_id}",
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


def create_session(name: str, dbid: str, password: str, access_token: str) -> dict[str, Any]:
    response = req.post(
        f"{DEVENV_URI}/{AURA_API_VERSION}/gds/sessions",
        json={"name": name, "dbid": dbid, "password": password},
        headers={"Authorization": f"Bearer {access_token}"}
    )
    print(response.text)
    response.raise_for_status()

    return response.json()["data"]  # type: ignore


def list_sessions(dbid: str, access_token: str) -> dict[str, Any]:
    response = req.get(
        f"{DEVENV_URI}/{AURA_API_VERSION}/gds/sessions",
        params={"instanceId": dbid},
        headers={"Authorization": f"Bearer {access_token}"}
    )
    print(response.text)
    response.raise_for_status()

    return response.json()["data"]  # type: ignore


def session_details(dbid: str, session_id: str, access_token: str) -> dict[str, Any]:
    response = req.get(f"{DEVENV_URI}/{AURA_API_VERSION}/gds/sessions/{session_id}", params={"instanceId": dbid}, headers={"Authorization": f"Bearer {access_token}"})

    print(response.text)
    response.raise_for_status()

    return response.json()["data"]  # type: ignore


def delete_session(dbid: str, session_id: str, access_token: str) -> dict[str, Any]:
    response = req.delete(f"{DEVENV_URI}/{AURA_API_VERSION}/gds/sessions/{session_id}", json={"instanceId": dbid}, headers={"Authorization": f"Bearer {access_token}"})
    response.raise_for_status()

    return response.json()["data"]  # type: ignore


def teardown_instance(access_token: str, db_id: str) -> None:
    TEARDOWN_MAX_WAIT_TIME = 10

    should_retry = True
    wait_time = 1

    while should_retry:
        sleep(wait_time)
        wait_time *= 2

        response = req.delete(
            f"{DEVENV_URI}/{AURA_API_VERSION}/instances/{db_id}",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        if response.status_code == 202:
            should_retry = False

        should_retry = (response.status_code in [500, 502, 503, 504]) and TEARDOWN_MAX_WAIT_TIME > wait_time

        if should_retry:
            logging.debug(f"Status code: {response.status_code} - Retrying in {wait_time} s")

    response.raise_for_status()


def wait_for_host(dbid: str, session_id: str, access_token: str) -> str:
    should_retry = True
    wait_time = 1

    while should_retry:
        details = session_details(dbid, session_id, access_token)

        should_retry = not details["host"]
        if should_retry:
            logging.debug(f"No host - Retrying in {wait_time} s")
            sleep(wait_time)
            wait_time *= 2

    return details["host"]


def bolt_connection(host: str, password: str) -> None:
    from neo4j import GraphDatabase
    # start a standard Neo4j Python Driver to connect to the AuraDB instance
    driver = GraphDatabase.driver(f"neo4j://{host}", auth=("neo4j", password))
    # try out our connection
    with driver.session() as session:
        print(session.run("RETURN true AS success").to_df())


def arrow_connection(host: str, password: str) -> None:
    import pyarrow.flight as flight
    location = flight.Location.for_grpc_tcp(host, 8491)
    client = flight.FlightClient(location)
    token_pair = client.authenticate_basic_token("neo4j", password)
    print(token_pair)


def main() -> None:
    access_token = get_access_token()
    logging.info("Access token for creation acquired")

    create_result = create_auradb_pro(access_token)
    dbid = create_result["id"]
    logging.info("Creation of database accepted")

    try:
        logging.info("Waiting for `%s` up and running", dbid)
        check_running(access_token, dbid)
        logging.info("Database `%s` up and running", dbid)
        res = list_sessions(dbid, access_token)
        logging.info(f"Listing sessions (expected empty): {res}")

        res = create_session("my-testing-session", dbid, "12345678", access_token)
        logging.info(f"Session created: {res}")

        session_id = res["id"]
        host = wait_for_host(dbid, session_id, access_token)
        logging.info(f"Session now has a host: {host}")

        bolt_connection(host, "12345678")
        arrow_connection(host, "12345678")

    finally:
        access_token = get_access_token()
        logging.info("Access token for teardown acquired")

        teardown_instance(access_token, dbid)
        delete_session(dbid, session_id)
        logging.info("Teardown of instance %s successful", dbid)


if __name__ == "__main__":
    import requests

    url = "https://api-devflorentin.neo4j-dev.io/v1beta5/gds/sessions"

    querystring = {"instanceId": "013b83df"}

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InhlUnhMWEZNY0lCbW8wX2dYekMzcSJ9.eyJ1c3IiOiIwNDI3NzM2Ni1iMTllLTRiODQtOTZhOS0zNTg4MWUwZjdkMDQiLCJpc3MiOiJodHRwczovL2F1cmEtYXBpLWRldi5ldS5hdXRoMC5jb20vIiwic3ViIjoiUFV2ZUJhSFpaVkdjUnJGWXRpNzVaM3dnZXZGbUZqd2VAY2xpZW50cyIsImF1ZCI6Imh0dHBzOi8vbm9ucHJvZC5uZW80ai5pbyIsImlhdCI6MTcxMDI1OTIzNSwiZXhwIjoxNzEwMjYyODM1LCJhenAiOiJQVXZlQmFIWlpWR2NSckZZdGk3NVozd2dldkZtRmp3ZSIsImd0eSI6ImNsaWVudC1jcmVkZW50aWFscyJ9.IzDFn8EHTSOnY_EL1PSWwWBDc09nEp9VrfzBcheD2KTRZsksQ0KSEpTy4IaOt7L5Ive-M29OVde8i5yqCwbMavxUr_QU3Z8Q-njZGlG2LVfKDMNsGm_lLcLGkSiP-6pErN0hPjKkKbzHYqQ_tqegs1H4Q3DjZG7Ou-ZUI6nYKbggx6dxL8-vN6xRwhXg_yJ5O5E-Pm5Ynhg0mAy0Is8hpg8TSzXZqqXRPeXwwoFVNNyzsJkbEffDAq3duCb4UT60PZvlHqEV7ovhUANLpE5adxhChYaiuwjP9bA_3Xjt9DiVueBLKWR8oqaPuV9vLcSKkKaDxd3XeoIjX6x4Ak_gbQ"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    print(response.text)


    main()




