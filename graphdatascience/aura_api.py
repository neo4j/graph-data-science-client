import logging
import time
from dataclasses import dataclass

import requests as req


class AuraApi:
    # FIXME allow to insert other for dev purpose
    base_uri = "https://api.neo4j.io/v1"

    def __init__(self, client_id: str, client_secret: str) -> None:
        self._credentials = (client_id, client_secret)
        self._token = None
        self._logger = logging.getLogger()

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
            "https://api.neo4j.io/v1/instances",
            json=data,
            headers={"Authorization": f"Bearer {self.__token()}"},
        )

        response.raise_for_status()

        return InstanceCreateDetails.fromJson(response.json()["data"])

    def delete_instance(self, instance_id: str) -> "InstanceDetails":
        response = req.delete(
            f"{AuraApi.base_uri}/instances/{instance_id}",
            headers={"Authorization": f"Bearer {self.__token()}"},
        )

        response.raise_for_status()

        return InstanceSpecificDetails.fromJson(response.json()["data"])

    def list_instances(self, instance_id: str = None) -> ["InstanceDetails"]:
        maybe_instance_id = f"/{instance_id}" if instance_id else ""

        response = req.get(
            f"{AuraApi.base_uri}/instances{maybe_instance_id}",
            headers={"Authorization": f"Bearer {self.__token()}"},
        )

        # TODO if the instance_id does not exist, we get a 404
        response.raise_for_status()

        raw_data = response.json()["data"]

        if instance_id:
            return InstanceSpecificDetails.fromJson(raw_data)
        else:
            return [InstanceDetails.fromJson(i) for i in raw_data]

    def _get_tenant_id(self) -> str:
        response = req.get(
            f"{AuraApi.base_uri}/tenants",
            headers={"Authorization": f"Bearer {self.__token()}"},
        )
        response.raise_for_status()

        raw_data = response.json()["data"]

        assert len(raw_data) == 1

        return raw_data[0]["id"]

    def _update_token(self) -> "AuraAuthToken":
        data = {
            "grant_type": "client_credentials",
        }

        self._logger.debug("Updating oauth token")

        response = req.post(
            "https://api.neo4j.io/oauth/token", data=data, auth=(self._credentials[0], self._credentials[1])
        )

        response.raise_for_status()

        return AuraAuthToken(response.json())


class AuraAuthToken:
    access_token: str
    expires_in: int
    token_type: str

    def __init__(self, json: dict) -> None:
        self.access_token = json["access_token"]
        self.expires_at = int(time.time()) + json["expires_in"]
        self.token_type = json["token_type"]

    def is_expired(self) -> bool:
        return self.expires_at >= int(time.time())


@dataclass(repr=True)
class InstanceDetails:
    id: str
    name: str
    tenant_id: str
    cloud_provider: str

    @classmethod
    def fromJson(cls, json: dict) -> "InstanceDetails":
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
    def fromJson(cls, json: dict) -> "InstanceSpecificDetails":
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
    def fromJson(cls, json: dict) -> "InstanceCreateDetails":
        return cls(
            id=json["id"],
            name=json.get("name", ""),
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            password=json["password"],
            username=json["username"],
            connection_url=json["connection_url"],
        )
