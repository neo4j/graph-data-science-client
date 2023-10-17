from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, List, Optional

import requests as req


@dataclass(repr=True)
class InstanceDetails:
    id: str
    name: str
    tenant_id: str
    cloud_provider: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
        )


@dataclass(repr=True)
class InstanceSpecificDetails(InstanceDetails):
    status: str
    connection_url: str
    memory: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceSpecificDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            status=json["status"],
            connection_url=json.get("connection_url", ""),
            memory=json["memory"],
        )


@dataclass(repr=True)
class InstanceCreateDetails(InstanceDetails):
    password: str
    username: str
    connection_url: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceCreateDetails:
        return cls(
            id=json["id"],
            name=json.get("name", ""),
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            password=json["password"],
            username=json["username"],
            connection_url=json["connection_url"],
        )


class AuraApi:
    # FIXME allow to insert other for dev purpose
    BASE_URI_V1 = "https://api.neo4j.io/v1"
    MAX_WAIT_TIME = 300

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
            return self.expires_at >= int(time.time())

    def __init__(self, client_id: str, client_secret: str) -> None:
        self._credentials = (client_id, client_secret)
        self._token: Optional[AuraApi.AuraAuthToken] = None
        self._logger = logging.getLogger()

    def _auth_token(self) -> str:
        if self._token is None or self._token.is_expired():
            self._token = self.__update_token()
        return self._token.access_token

    def create_instance(self, name: str) -> InstanceCreateDetails:
        # TODO should give more control here
        data = {
            "name": name,
            "memory": "8GB",
            "version": "5",
            "region": "europe-west1",
            "type": "professional-ds",
            "tenant_id": self._get_tenant_id(),
            "cloud_provider": "gcp",
        }

        response = req.post(
            "https://api.neo4j.io/v1/instances",
            json=data,
            headers={"Authorization": f"Bearer {self._auth_token()}"},
        )

        response.raise_for_status()

        return InstanceCreateDetails.fromJson(response.json()["data"])

    def delete_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = req.delete(
            f"{AuraApi.BASE_URI_V1}/instances/{instance_id}",
            headers={"Authorization": f"Bearer {self._auth_token()}"},
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        return InstanceSpecificDetails.fromJson(response.json()["data"])

    def list_instances(self) -> List[InstanceDetails]:
        response = req.get(
            f"{AuraApi.BASE_URI_V1}/instances",
            headers={"Authorization": f"Bearer {self._auth_token()}"},
        )

        response.raise_for_status()

        raw_data = response.json()["data"]

        return [InstanceDetails.fromJson(i) for i in raw_data]

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = req.get(
            f"{AuraApi.BASE_URI_V1}/instances/{instance_id}",
            headers={"Authorization": f"Bearer {self._auth_token()}"},
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        raw_data = response.json()["data"]

        return InstanceSpecificDetails.fromJson(raw_data)

    def wait_for_instance_running(self, instance_id: str, sleep_time: float = 0.2) -> bool:
        while sleep_time < AuraApi.MAX_WAIT_TIME:
            instance = self.list_instance(instance_id)
            if instance is None or instance.status == "DELETING":
                return False
            if instance.status == "RUNNING":
                return True
            self._logger.debug(
                f"Instance {instance_id} is not running yet, but {instance.status}. Retry in {sleep_time} seconds."
            )
            time.sleep(sleep_time)

        return False

    def _get_tenant_id(self) -> str:
        response = req.get(
            f"{AuraApi.BASE_URI_V1}/tenants",
            headers={"Authorization": f"Bearer {self._auth_token()}"},
        )
        response.raise_for_status()

        raw_data = response.json()["data"]

        assert len(raw_data) == 1

        return raw_data[0]["id"]  # type: ignore

    def __update_token(self) -> AuraAuthToken:
        data = {
            "grant_type": "client_credentials",
        }

        self._logger.debug("Updating oauth token")

        response = req.post(
            "https://api.neo4j.io/oauth/token", data=data, auth=(self._credentials[0], self._credentials[1])
        )

        response.raise_for_status()

        return AuraApi.AuraAuthToken(response.json())
