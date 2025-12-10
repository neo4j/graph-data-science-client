import logging
import time
from time import sleep
from typing import Any

import requests


class AuraApiCI:
    class AuraAuthToken:
        access_token: str
        expires_in: int
        token_type: str

        def __init__(self, json: dict[str, Any]) -> None:
            self.access_token = json["access_token"]
            expires_in: int = json["expires_in"]
            self.expires_at = int(time.time()) + expires_in
            self.token_type = json["token_type"]

        def is_expired(self) -> bool:
            return self.expires_at <= int(time.time())

    def __init__(self, client_id: str, client_secret: str, project_id: str | None = None) -> None:
        self._token: AuraApiCI.AuraAuthToken | None = None
        self._logger = logging.getLogger()
        self._auth = (client_id, client_secret)
        self._project_id = project_id

    def _build_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._auth_token()}", "User-agent": "neo4j-graphdatascience-ci"}

    def _auth_token(self) -> str:
        if self._token is None or self._token.is_expired():
            self._token = self._update_token()
        return self._token.access_token

    def _update_token(self) -> AuraAuthToken:
        data = {
            "grant_type": "client_credentials",
        }

        self._logger.debug("Updating oauth token")

        response = requests.post("https://api-staging.neo4j.io/oauth/token", data=data, auth=self._auth)

        response.raise_for_status()

        return AuraApiCI.AuraAuthToken(response.json())

    def create_instance(self, name: str, memory: str, type: str) -> dict[str, Any]:
        CREATE_OK_MAX_WAIT_TIME = 10

        data = {
            "name": name,
            "memory": memory,
            "version": "5",
            "region": "europe-west1",
            "type": type,
            "cloud_provider": "gcp",
            "tenant_id": self.get_tenant_id(),
        }

        should_retry = True
        wait_time = 1

        while should_retry:
            sleep(wait_time)
            wait_time *= 2

            response = requests.post(
                "https://api-staging.neo4j.io/v1/instances",
                json=data,
                headers=self._build_header(),
            )
            should_retry = response.status_code in [500, 502, 503, 504, 405] and CREATE_OK_MAX_WAIT_TIME > wait_time

            if should_retry:
                logging.debug(f"Error code: {response.status_code} - Retrying in {wait_time} s")

        response_json = response.json()
        if "errors" in response_json:
            raise Exception(response_json["errors"])

        return response_json["data"]  # type: ignore

    def check_running(self, db_id: str) -> None:
        RUNNING_MAX_WAIT_TIME = 60 * 5

        should_retry = True
        wait_time = 1

        while should_retry:
            sleep(wait_time)
            wait_time *= 2
            wait_time = min(wait_time, 20)

            response = requests.get(
                f"https://api-staging.neo4j.io/v1/instances/{db_id}",
                headers=self._build_header(),
            )

            instance_status = "?"
            if response.status_code == 200:
                instance_status = response.json()["data"]["status"]

            should_retry = (
                response.status_code in [500, 502, 503, 504] or instance_status == "creating"
            ) and RUNNING_MAX_WAIT_TIME > wait_time

            if should_retry:
                logging.debug(
                    f"Status code: {response.status_code}, Status: {instance_status} - Retrying in {wait_time} s"
                )

        response.raise_for_status()

    def teardown_instance(self, db_id: str) -> bool:
        TEARDOWN_MAX_WAIT_TIME = 10

        should_retry = True
        wait_time = 1

        while should_retry:
            sleep(wait_time)
            wait_time *= 2

            response = requests.delete(
                f"https://api-staging.neo4j.io/v1/instances/{db_id}",
                headers=self._build_header(),
            )

            if response.status_code == 202:
                should_retry = False

            should_retry = (response.status_code in [500, 502, 503, 504]) and TEARDOWN_MAX_WAIT_TIME > wait_time

            if should_retry:
                logging.debug(f"Status code: {response.status_code} - Retrying in {wait_time} s")

        if response.status_code == 404:
            return False

        response.raise_for_status()

        return True

    def get_tenant_id(self) -> str:
        if self._project_id:
            return self._project_id

        response = requests.get(
            "https://api-staging.neo4j.io/v1/tenants",
            headers=self._build_header(),
        )
        response.raise_for_status()

        raw_data = response.json()["data"]
        assert len(raw_data) == 1

        return raw_data[0]["id"]  # type: ignore
