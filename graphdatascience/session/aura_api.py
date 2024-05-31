from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests as req
from requests import HTTPError

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    SessionDetails,
    TenantDetails,
    WaitResult,
)
from graphdatascience.version import __version__


class AuraApi:
    class AuraAuthToken:
        access_token: str
        expires_in: int
        token_type: str

        def __init__(self, json: Dict[str, Any]) -> None:
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

    def create_session(self, name: str, dbid: str, pwd: str, memory: str) -> SessionDetails:
        response = req.post(
            f"{self._base_uri}/v1beta5/data-science/sessions",
            headers=self._build_header(),
            json={"name": name, "instance_id": dbid, "password": pwd, "memory": memory},
        )

        response.raise_for_status()

        return SessionDetails.fromJson(response.json())

    def list_session(self, session_id: str, dbid: str) -> Optional[SessionDetails]:
        response = req.get(
            f"{self._base_uri}/v1beta5/data-science/sessions/{session_id}?instanceId={dbid}",
            headers=self._build_header(),
        )

        if response.status_code == 404:
            return None

        response.raise_for_status()

        return SessionDetails.fromJson(response.json())

    def list_sessions(self, dbid: str) -> List[SessionDetails]:
        response = req.get(
            f"{self._base_uri}/v1beta5/data-science/sessions?instanceId={dbid}",
            headers=self._build_header(),
        )

        response.raise_for_status()

        return [SessionDetails.fromJson(s) for s in response.json()]

    def wait_for_session_running(
        self,
        session_id: str,
        dbid: str,
        sleep_time: float = 0.2,
        max_sleep_time: float = 10,
        max_wait_time: float = 300,
    ) -> WaitResult:
        waited_time = 0.0
        while waited_time < max_wait_time:
            session = self.list_session(session_id, dbid)
            if session is None:
                return WaitResult.from_error(f"Session `{session_id}` for database `{dbid}` not found -- please retry")
            elif session.status == "Ready" and session.host:  # check host needed until dns based routing
                return WaitResult.from_connection_url(session.bolt_connection_url())
            else:
                self._logger.debug(
                    f"Session `{session_id}` is not yet running. "
                    f"Current status: {session.status} Host: {session.host}. "
                    f"Retrying in {sleep_time} seconds..."
                )
            waited_time += sleep_time
            time.sleep(sleep_time)
            sleep_time = min(sleep_time * 2, max_sleep_time, max_wait_time - waited_time)

        return WaitResult.from_error(
            f"Session `{session_id}` for database `{dbid}` is not running after {waited_time} seconds"
        )

    def delete_session(self, session_id: str, dbid: str) -> bool:
        response = req.delete(
            f"{self._base_uri}/v1beta5/data-science/sessions/{session_id}",
            headers=self._build_header(),
            json={"instance_id": dbid},
        )

        if response.status_code == 404:
            return False
        elif response.status_code == 202:
            return True

        response.raise_for_status()

        return False

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
        self, instance_id: str, sleep_time: float = 0.2, max_sleep_time: float = 10, max_wait_time: float = 300
    ) -> WaitResult:
        waited_time = 0.0
        while waited_time < max_wait_time:
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
            sleep_time = min(sleep_time * 2, max_sleep_time, max_wait_time - waited_time)

        return WaitResult.from_error(f"Instance is not running after waiting for {waited_time} seconds")

    def estimate_size(
        self, node_count: int, relationship_count: int, algorithm_categories: List[AlgorithmCategory]
    ) -> EstimationDetails:
        data = {
            "node_count": node_count,
            "relationship_count": relationship_count,
            "algorithm_categories": [i.value for i in algorithm_categories],
            "instance_type": "dsenterprise",
        }

        response = req.post(f"{self._base_uri}/v1/instances/sizing", headers=self._build_header(), json=data)
        response.raise_for_status()

        return EstimationDetails.from_json(response.json()["data"])

    def _get_tenant_id(self) -> str:
        response = req.get(
            f"{self._base_uri}/v1/tenants",
            headers=self._build_header(),
        )
        response.raise_for_status()

        raw_data = response.json()["data"]

        if len(raw_data) != 1:
            tenants_dict = {d["id"]: d["name"] for d in raw_data}
            raise RuntimeError(
                f"This account has access to multiple tenants: `{tenants_dict}`. Please specify which one to use."
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

    def _build_header(self) -> Dict[str, str]:
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
