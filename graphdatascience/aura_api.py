from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import urlparse

import requests as req
from requests import HTTPError

from .version import __version__


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
    type: str
    region: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceSpecificDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            status=json["status"],
            connection_url=json.get("connection_url", ""),
            memory=json.get("memory", ""),
            type=json["type"],
            region=json["region"],
        )


@dataclass(repr=True)
class InstanceCreateDetails(InstanceDetails):
    password: str
    username: str
    connection_url: str
    type: str
    region: str

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
            type=json.get("type", ""),
            region=json["region"],
        )


class AuraApi:
    DEV_ENV = os.environ.get("AURA_ENV")
    BASE_URI = "https://api.neo4j.io" if not DEV_ENV else f"https://api-{os.environ.get('AURA_ENV')}.neo4j-dev.io"

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

    def __init__(self, client_id: str, client_secret: str, tenant_id: Optional[str] = None) -> None:
        self._credentials = (client_id, client_secret)
        self._token: Optional[AuraApi.AuraAuthToken] = None
        self._logger = logging.getLogger()
        self._tenant_id = tenant_id if tenant_id else self._get_tenant_id()

    @staticmethod
    def extract_id(uri: str) -> str:
        host = urlparse(uri).hostname

        if not host:
            raise RuntimeError(f"Could not parse the uri `{uri}`.")

        return host.split(".")[0].split("-")[0]

    def create_instance(self, name: str, cloud_provider: str, region: str) -> InstanceCreateDetails:
        # TODO should give more control here
        data = {
            "name": name,
            "memory": "8GB",
            "version": "5",
            "region": region,
            # TODO should be figured out from the tenant details in the future
            "type": "enterprise-ds" if not AuraApi.DEV_ENV else "professional-ds",
            "tenant_id": self._tenant_id,
            "cloud_provider": cloud_provider,
        }

        response = req.post(
            f"{AuraApi.BASE_URI}/v1/instances",
            json=data,
            headers=self._build_header(),
        )

        try:
            response.raise_for_status()
        except HTTPError as e:
            print(response.json())
            raise e

        return InstanceCreateDetails.fromJson(response.json()["data"])

    def delete_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = req.delete(
            f"{AuraApi.BASE_URI}/v1/instances/{instance_id}",
            headers=self._build_header(),
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        return InstanceSpecificDetails.fromJson(response.json()["data"])

    def list_instances(self) -> List[InstanceDetails]:
        response = req.get(
            f"{AuraApi.BASE_URI}/v1/instances",
            headers=self._build_header(),
            params={"tenantId": self._tenant_id},
        )

        response.raise_for_status()

        raw_data = response.json()["data"]

        return [InstanceDetails.fromJson(i) for i in raw_data]

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = req.get(
            f"{AuraApi.BASE_URI}/v1/instances/{instance_id}",
            headers=self._build_header(),
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        raw_data = response.json()["data"]

        return InstanceSpecificDetails.fromJson(raw_data)

    def wait_for_instance_running(
        self, instance_id: str, sleep_time: float = 0.2, max_sleep_time: float = 300
    ) -> Optional[str]:
        waited_time = 0.0
        while waited_time <= max_sleep_time:
            instance = self.list_instance(instance_id)
            if instance is None:
                return "Instance is not found -- please retry"
            elif instance.status in ["deleting", "destroying"]:
                return "Instance is being deleted"
            elif instance.status == "running":
                return None
            else:
                self._logger.debug(
                    f"Instance `{instance_id}` is not yet running. "
                    f"Current status: {instance.status}. "
                    f"Retrying in {sleep_time} seconds..."
                )
            waited_time += sleep_time
            time.sleep(sleep_time)

        return f"Instance is not running after waiting for {waited_time} seconds"

    def _get_tenant_id(self) -> str:
        response = req.get(
            f"{AuraApi.BASE_URI}/v1/tenants",
            headers=self._build_header(),
        )
        response.raise_for_status()

        raw_data = response.json()["data"]

        if len(raw_data) != 1:
            raise RuntimeError(
                f"This account has access to multiple tenants `{raw_data}`. Please specify which one to use."
            )

        return raw_data[0]["id"]  # type: ignore

    def _build_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._auth_token()}", "User-agent": f"neo4j-graphdatascience-v{__version__}"}

    def _auth_token(self) -> str:
        if self._token is None or self._token.is_expired():
            self._token = self._update_token()
        return self._token.access_token

    def _update_token(self) -> AuraAuthToken:
        data = {
            "grant_type": "client_credentials",
        }

        self._logger.debug("Updating oauth token")

        response = req.post(
            f"{AuraApi.BASE_URI}/oauth/token", data=data, auth=(self._credentials[0], self._credentials[1])
        )

        response.raise_for_status()

        return AuraApi.AuraAuthToken(response.json())
