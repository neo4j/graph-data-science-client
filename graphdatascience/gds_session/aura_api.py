from __future__ import annotations

import dataclasses
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, List, NamedTuple, Optional, Set
from urllib.parse import urlparse

import requests as req
from requests import HTTPError

from graphdatascience.version import __version__


@dataclass(repr=True, frozen=True)
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


@dataclass(repr=True, frozen=True)
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


@dataclass(repr=True, frozen=True)
class InstanceCreateDetails:
    id: str
    username: str
    password: str
    connection_url: str

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> InstanceCreateDetails:
        fields = dataclasses.fields(cls)
        if any(f.name not in json for f in fields):
            raise RuntimeError(f"Missing required field. Expected `{[f.name for f in fields]}` but got `{json}`")

        return cls(**{f.name: json[f.name] for f in fields})


class WaitResult(NamedTuple):
    connection_url: str
    error: str

    @classmethod
    def from_error(cls, error: str) -> WaitResult:
        return cls(connection_url="", error=error)

    @classmethod
    def from_connection_url(cls, connection_url: str) -> WaitResult:
        return cls(connection_url=connection_url, error="")


@dataclass(repr=True, frozen=True)
class TenantDetails:
    id: str
    ds_type: str
    regions_per_provider: dict[str, Set[str]]

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> TenantDetails:
        regions_per_provider = defaultdict(set)
        instance_types = set()
        ds_type = None

        for configs in json["instance_configurations"]:
            type = configs["type"]
            if type.split("-")[1] == "ds":
                regions_per_provider[configs["cloud_provider"]].add(configs["region"])
                ds_type = type
            instance_types.add(configs["type"])

        id = json["id"]
        if not ds_type:
            raise RuntimeError(
                f"Tenant with id `{id}` cannot create DS instances. Available instances are `{instance_types}`."
            )

        return cls(
            id=id,
            ds_type=ds_type,
            regions_per_provider=regions_per_provider,
        )


class AuraApi:
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
        self._dev_env = os.environ.get("AURA_ENV")

        if not self._dev_env:
            self._base_uri = "https://api.neo4j.io"
        elif self._dev_env == "staging":
            self._base_uri = "https://api-staging.neo4j.io"
        else:
            self._base_uri = f"https://api-{self._dev_env}.neo4j-dev.io"

        self._credentials = (client_id, client_secret)
        self._token: Optional[AuraApi.AuraAuthToken] = None
        self._logger = logging.getLogger()
        self._tenant_id = tenant_id if tenant_id else self._get_tenant_id()
        self._tenant_details: Optional[TenantDetails] = None

    @staticmethod
    def extract_id(uri: str) -> str:
        host = urlparse(uri).hostname

        if not host:
            raise RuntimeError(f"Could not parse the uri `{uri}`.")

        return host.split(".")[0].split("-")[0]

    def create_instance(self, name: str, memory: str, cloud_provider: str, region: str) -> InstanceCreateDetails:
        tenant_details = self.tenant_details()

        data = {
            "name": name,
            "memory": memory,
            "version": "5",
            "region": region,
            "type": tenant_details.ds_type,
            "tenant_id": self._tenant_id,
            "cloud_provider": cloud_provider,
        }

        response = req.post(
            f"{self._base_uri}/v1/instances",
            json=data,
            headers=self._build_header(),
        )

        try:
            response.raise_for_status()
        except HTTPError as e:
            print(response.json())
            raise e

        return InstanceCreateDetails.from_json(response.json()["data"])

    def delete_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = req.delete(
            f"{self._base_uri}/v1/instances/{instance_id}",
            headers=self._build_header(),
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        return InstanceSpecificDetails.fromJson(response.json()["data"])

    def list_instances(self) -> List[InstanceDetails]:
        response = req.get(
            f"{self._base_uri}/v1/instances",
            headers=self._build_header(),
            params={"tenantId": self._tenant_id},
        )

        response.raise_for_status()

        raw_data = response.json()["data"]

        return [InstanceDetails.fromJson(i) for i in raw_data]

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = req.get(
            f"{self._base_uri}/v1/instances/{instance_id}",
            headers=self._build_header(),
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        raw_data = response.json()["data"]

        return InstanceSpecificDetails.fromJson(raw_data)

    def wait_for_instance_running(
        self, instance_id: str, sleep_time: float = 0.2, max_sleep_time: float = 300
    ) -> WaitResult:
        waited_time = 0.0
        while waited_time <= max_sleep_time:
            instance = self.list_instance(instance_id)
            if instance is None:
                return WaitResult.from_error("Instance is not found -- please retry")
            elif instance.status in ["deleting", "destroying"]:
                return WaitResult.from_error("Instance is being deleted")
            elif instance.status == "running":
                return WaitResult.from_connection_url(instance.connection_url)
            else:
                self._logger.debug(
                    f"Instance `{instance_id}` is not yet running. "
                    f"Current status: {instance.status}. "
                    f"Retrying in {sleep_time} seconds..."
                )
            waited_time += sleep_time
            time.sleep(sleep_time)

        return WaitResult.from_error(f"Instance is not running after waiting for {waited_time} seconds")

    def _get_tenant_id(self) -> str:
        response = req.get(
            f"{self._base_uri}/v1/tenants",
            headers=self._build_header(),
        )
        response.raise_for_status()

        raw_data = response.json()["data"]

        if len(raw_data) != 1:
            raise RuntimeError(
                f"This account has access to multiple tenants `{raw_data}`. Please specify which one to use."
            )

        return raw_data[0]["id"]  # type: ignore

    def tenant_details(self) -> TenantDetails:
        if not self._tenant_details:
            response = req.get(
                f"{self._base_uri}/v1/tenants/{self._tenant_id}",
                headers=self._build_header(),
            )
            response.raise_for_status()
            self._tenant_details = TenantDetails.from_json(response.json()["data"])
        return self._tenant_details

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
            f"{self._base_uri}/oauth/token", data=data, auth=(self._credentials[0], self._credentials[1])
        )

        response.raise_for_status()

        return AuraApi.AuraAuthToken(response.json())

    def _instance_type(self) -> str:
        return "enterprise-ds" if not self._dev_env else "professional-ds"
