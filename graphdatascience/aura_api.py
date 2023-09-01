import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests as req


class AuraApi:
    def __init__(
        self, client_id: str, client_secret: str, environment: Optional[str] = None, api_version: str = "v1"
    ) -> None:
        self._credentials = (client_id, client_secret)
        self._token: Optional[AuraAuthToken] = None
        self._logger = logging.getLogger()
        self._api_version = api_version

        if environment:
            self._base_uri = f"https://api-{environment}.neo4j-dev.io"
        else:
            self._base_uri = "https://api.neo4j.io"

    def __token(self) -> str:
        if self._token is None:
            self._token = self._update_token()
        return self._token.access_token

    def create_instance(self, name: str) -> "InstanceCreateDetails":
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
            f"{self._base_uri}/{self._api_version}/instances",
            json=data,
            headers={"Authorization": f"Bearer {self.__token()}"},
        )

        response.raise_for_status()

        return InstanceCreateDetails.from_json(response.json()["data"])

    def delete_instance(self, instance_id: str) -> "InstanceDetails":
        response = req.delete(
            f"{self._base_uri}/{self._api_version}/instances/{instance_id}",
            headers={"Authorization": f"Bearer {self.__token()}"},
        )

        response.raise_for_status()

        return InstanceSpecificDetails.from_json(response.json()["data"])

    def list_instances(self, instance_id: Optional[str] = None) -> List["InstanceDetails"]:
        maybe_instance_id = f"/{instance_id}" if instance_id else ""

        response = req.get(
            f"{self._base_uri}/{self._api_version}/instances{maybe_instance_id}",
            headers={"Authorization": f"Bearer {self.__token()}"},
        )

        # TODO if the instance_id does not exist, we get a 404
        response.raise_for_status()

        raw_data = response.json()["data"]

        if instance_id:
            return [InstanceSpecificDetails.from_json(raw_data)]
        else:
            return [InstanceDetails.from_json(i) for i in raw_data]

    def _get_tenant_id(self) -> str:
        response = req.get(
            f"{self._base_uri}/{self._api_version}/tenants",
            headers={"Authorization": f"Bearer {self.__token()}"},
        )
        response.raise_for_status()

        raw_data = response.json()["data"]

        assert len(raw_data) == 1

        return raw_data[0]["id"]  # type: ignore

    def _update_token(self) -> "AuraAuthToken":
        data = {
            "grant_type": "client_credentials",
        }

        self._logger.debug("Updating oauth token")

        response = req.post(
            f"{self._base_uri}/oauth/token", data=data, auth=(self._credentials[0], self._credentials[1])
        )

        response.raise_for_status()

        return AuraAuthToken.from_json(response.json())


@dataclass(repr=True)
class AuraAuthToken:
    access_token: str
    expires_at: int
    token_type: str

    def is_expired(self) -> bool:
        return self.expires_at >= int(time.time())

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> "AuraAuthToken":
        return cls(
            access_token=json["access_token"],
            expires_at=int(time.time()) + json["expires_in"],
            token_type=json["token_type"],
        )


@dataclass(repr=True)
class InstanceDetails:
    id: str
    name: str
    tenant_id: str
    cloud_provider: str

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> "InstanceDetails":
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
    def from_json(cls, json: Dict[str, Any]) -> "InstanceSpecificDetails":
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

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> "InstanceCreateDetails":
        return cls(
            id=json["id"],
            name=json.get("name", ""),
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            password=json["password"],
            username=json["username"],
        )
