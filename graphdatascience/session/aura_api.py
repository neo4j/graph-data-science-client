from __future__ import annotations

import logging
import os
import time
import warnings
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import requests.auth

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
from graphdatascience.session.session_sizes import SessionMemoryValue
from graphdatascience.version import __version__


class AuraApiError(Exception):
    def __init__(self, message: str, status_code: int):
        super().__init__(self, message)
        self.status_code = status_code
        self.message = message


class AuraApi:

    def __init__(self, base_uri: str, client_id: str, client_secret: str, tenant_id: Optional[str] = None) -> None:
        self._base_uri = base_uri

        self._auth = AuraApi.Auth(oauth_url=f"{self._base_uri}/oauth/token", credentials=(client_id, client_secret))
        self._logger = logging.getLogger()

        self._request_session = requests.Session()
        self._request_session.headers = {"User-agent": f"neo4j-graphdatascience-v{__version__}"}
        self._request_session.auth = self._auth

        self._tenant_id = tenant_id if tenant_id else self._get_tenant_id()
        self._tenant_details: Optional[TenantDetails] = None

    @classmethod
    def create(cls, client_id: str, client_secret: str, tenant_id: Optional[str] = None) -> AuraApi:
        aura_env = os.environ.get("AURA_ENV")

        if not aura_env or aura_env == "production":
            base_uri = "https://api.neo4j.io"
        elif aura_env == "staging":
            base_uri = "https://api-staging.neo4j.io"
        else:
            base_uri = f"https://api-{aura_env}.neo4j-dev.io"

        return cls(base_uri, client_id, client_secret, tenant_id)

    @staticmethod
    def extract_id(uri: str) -> str:
        host = urlparse(uri).hostname

        if not host:
            raise RuntimeError(f"Could not parse the uri `{uri}`.")

        return host.split(".")[0].split("-")[0]

    def create_session(self, name: str, dbid: str, pwd: str, memory: SessionMemoryValue) -> SessionDetails:
        response = self._request_session.post(
            f"{self._base_uri}/v1beta5/data-science/sessions",
            json={"name": name, "instance_id": dbid, "password": pwd, "memory": memory.value},
        )

        self._check_resp(response)

        return SessionDetails.fromJson(response.json()["data"])

    def list_session(self, session_id: str) -> Optional[SessionDetails]:
        response = self._request_session.get(f"{self._base_uri}/v1beta5/data-science/sessions/{session_id}")

        self._check_resp(response)

        return SessionDetails.fromJson(response.json()["data"])

    def list_sessions(self, dbid: Optional[str] = None) -> List[SessionDetails]:
        params = {
            "tenantId": self._tenant_id,
            "instanceId": dbid,
        }

        response = self._request_session.get(f"{self._base_uri}/v1beta5/data-science/sessions", params=params)

        self._check_resp(response)

        return [SessionDetails.fromJson(s) for s in response.json()["data"]]

    def wait_for_session_running(
        self,
        session_id: str,
        sleep_time: float = 0.2,
        max_sleep_time: float = 10,
        max_wait_time: float = 300,
    ) -> WaitResult:
        waited_time = 0.0
        while waited_time < max_wait_time:
            session = self.list_session(session_id)
            if session is None:
                return WaitResult.from_error(f"Session `{session_id}` not found -- please retry")
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

        return WaitResult.from_error(f"Session `{session_id}` is not running after {waited_time} seconds")

    def delete_session(self, session_id: str) -> bool:
        response = self._request_session.delete(
            f"{self._base_uri}/v1beta5/data-science/sessions/{session_id}",
        )

        self._check_endpoint_expiry(response)

        if response.status_code == 404:
            return False
        elif response.status_code == 202:
            return True
        self._check_endpoint_expiry(response)

        return False

    def create_instance(
        self, name: str, memory: SessionMemoryValue, cloud_provider: str, region: str
    ) -> InstanceCreateDetails:
        tenant_details = self.tenant_details()

        data = {
            "name": name,
            "memory": memory.value,
            "version": "5",
            "region": region,
            "type": tenant_details.ds_type,
            "tenant_id": self._tenant_id,
            "cloud_provider": cloud_provider,
        }

        response = self._request_session.post(f"{self._base_uri}/v1/instances", json=data)

        self._check_resp(response)

        return InstanceCreateDetails.from_json(response.json()["data"])

    def delete_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = self._request_session.delete(f"{self._base_uri}/v1/instances/{instance_id}")

        if response.status_code == 404:
            return None

        self._check_resp(response)

        return InstanceSpecificDetails.fromJson(response.json()["data"])

    def list_instances(self) -> List[InstanceDetails]:
        response = self._request_session.get(f"{self._base_uri}/v1/instances", params={"tenantId": self._tenant_id})

        self._check_resp(response)

        raw_data = response.json()["data"]

        return [InstanceDetails.fromJson(i) for i in raw_data]

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        response = self._request_session.get(f"{self._base_uri}/v1/instances/{instance_id}")

        if response.status_code == 404:
            return None

        self._check_resp(response)

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

        response = self._request_session.post(f"{self._base_uri}/v1/instances/sizing", json=data)
        self._check_resp(response)

        return EstimationDetails.from_json(response.json()["data"])

    def _get_tenant_id(self) -> str:
        response = self._request_session.get(f"{self._base_uri}/v1/tenants")
        self._check_resp(response)

        raw_data = response.json()["data"]

        if len(raw_data) != 1:
            tenants_dict = {d["id"]: d["name"] for d in raw_data}
            raise RuntimeError(
                f"This account has access to multiple tenants: `{tenants_dict}`. Please specify which one to use."
            )

        return raw_data[0]["id"]  # type: ignore

    def tenant_details(self) -> TenantDetails:
        if not self._tenant_details:
            response = self._request_session.get(f"{self._base_uri}/v1/tenants/{self._tenant_id}")
            self._check_resp(response)
            self._tenant_details = TenantDetails.from_json(response.json()["data"])
        return self._tenant_details

    def _check_resp(self, resp: requests.Response) -> None:
        self._check_status_code(resp)
        self._check_endpoint_expiry(resp)

    def _check_status_code(self, resp: requests.Response) -> None:
        if resp.status_code >= 400:
            raise AuraApiError(
                f"Request for {resp.url} failed with status code {resp.status_code} - {resp.reason}: {resp.text}",
                status_code=resp.status_code,
            )

    def _check_endpoint_expiry(self, resp: requests.Response) -> None:
        expiry_date = resp.headers.get("X-Tyk-Api-Expires")
        if expiry_date:
            warnings.warn(
                f"The endpoint is deprecated and will be removed on {expiry_date}."
                " Please update to a newer version of this client.",
                DeprecationWarning,
            )

    class Auth(requests.auth.AuthBase):
        class Token:
            access_token: str
            expires_in: int
            token_type: str

            def __init__(self, json: Dict[str, Any]) -> None:
                self.access_token = json["access_token"]
                self.token_type = json["token_type"]

                expires_in: int = json["expires_in"]
                refresh_in: int = expires_in if expires_in <= 10 else expires_in - 10
                # avoid token expiry during request send by refreshing 10 seconds earlier
                self.refresh_at = int(time.time()) + refresh_in

            def should_refresh(self) -> bool:
                return self.refresh_at <= int(time.time())

        def __init__(self, oauth_url: str, credentials: Tuple[str, str]) -> None:
            self._token: Optional[AuraApi.Auth.Token] = None
            self._logger = logging.getLogger()
            self._oauth_url = oauth_url
            self._credentials = credentials

        def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
            r.headers["Authorization"] = f"Bearer {self._auth_token()}"
            return r

        def _auth_token(self) -> str:
            if self._token is None or self._token.should_refresh():
                self._token = self._update_token()
            return self._token.access_token

        def _update_token(self) -> AuraApi.Auth.Token:
            data = {
                "grant_type": "client_credentials",
            }

            self._logger.debug("Updating oauth token")

            resp = requests.post(self._oauth_url, data=data, auth=(self._credentials[0], self._credentials[1]))

            if resp.status_code >= 400:
                raise AuraApiError(
                    "Failed to authorize with provided client credentials: "
                    + f"{resp.status_code} - {resp.reason}, {resp.text}",
                    status_code=resp.status_code,
                )

            return AuraApi.Auth.Token(resp.json())
