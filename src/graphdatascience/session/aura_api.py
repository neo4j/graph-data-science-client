from __future__ import annotations

import logging
import math
import time
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

import requests
import requests.auth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    ProjectDetails,
    SessionDetails,
    SessionDetailsWithErrors,
    SessionErrorData,
    WaitResult,
)
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.session_sizes import SessionMemoryValue
from graphdatascience.version import __version__


class AuraApiError(Exception):
    """
    Raised when an API call to the AuraAPI fails (after retries).
    """

    def __init__(self, message: str, status_code: int):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class SessionStatusError(Exception):
    """
    Raised when a session is in a non-healthy state. Such as after a session failed or was deleted.
    """

    def __init__(
        self,
        errors: list[SessionErrorData],
        details: SessionDetails | None = None,
        hint: str | None = None,
    ):
        message = f"Session is in an unhealthy state. Details: {[str(e) for e in errors]}"

        for line in self._context_lines(errors, details, hint):
            message += f"\n\t{line}"

        super().__init__(message)

    @staticmethod
    def _context_lines(errors: list[SessionErrorData], details: SessionDetails | None, hint: str | None) -> list[str]:
        lines: list[str] = []

        if details:
            lines.append(
                f"Session `{details.name}` (id `{details.id}`) has status `{details.status}`"
                f" and memory `{details.memory.value}`."
            )

        if details and details.termination_reason:
            lines.append(f"Termination reason: `{details.termination_reason}`.")

        if hint:
            lines.append(hint)

        if any(error.is_out_of_memory() for error in errors):
            lines.append(
                "A session cannot recover from running out of memory."
                " Create a new, larger one; use `GdsSessions.estimate` to size it."
            )

        return lines


class AuraApi:
    API_VERSION = "v1"
    # Health checks are often run while something else already failed, possibly because of a
    # network problem. They must not add a long delay before the original error is reported.
    HEALTH_CHECK_TIMEOUT = (5.0, 30.0)

    # (connect, read) timeouts in seconds, applied per attempt. These are a guard against a
    # connection stalling indefinitely.
    DEFAULT_TIMEOUT = (10.0, 120.0)

    def __init__(
        self, client_id: str, client_secret: str, project_id: str | None = None, aura_env: str | None = None
    ) -> None:
        self._base_uri = AuraApi.base_uri(aura_env)
        self._credentials = (client_id, client_secret)

        self._auth = AuraApi.Auth(
            oauth_url=f"{self._base_uri}/oauth/token",
            credentials=self._credentials,
            headers={"User-agent": f"neo4j-graphdatascience-v{__version__}"},
        )
        self._request_session = self._init_request_session()
        # used for health checks, which should fail fast instead of retrying for a long time
        self._health_check_session = self._init_request_session(total_retries=1)
        self._logger = logging.getLogger()

        self._project_id = project_id if project_id else self._get_project_id()
        self._project_details: ProjectDetails | None = None

    def _init_request_session(self, total_retries: int = 4) -> requests.Session:
        request_session = AuraApi.TokenRefreshSession(self._auth)
        request_session.headers = {"User-agent": f"neo4j-graphdatascience-v{__version__}"}
        request_session.auth = self._auth
        # dont retry on POST as its not idempotent
        request_session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    allowed_methods=["GET", "DELETE"],
                    # These retries reuse the token that was signed in before the first attempt
                    total=total_retries,
                    status_forcelist=[
                        HTTPStatus.TOO_MANY_REQUESTS.value,
                        HTTPStatus.INTERNAL_SERVER_ERROR.value,
                        HTTPStatus.BAD_GATEWAY.value,
                        HTTPStatus.SERVICE_UNAVAILABLE.value,
                        HTTPStatus.GATEWAY_TIMEOUT.value,
                    ],
                    backoff_factor=0.1,
                )
            ),
        )
        return request_session

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        # `requests.Session` only serializes its own attributes, so a `TokenRefreshSession` would
        # arrive without the auth it needs to refresh a rejected token. We rebuild them instead.
        del state["_request_session"]
        del state["_health_check_session"]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._request_session = self._init_request_session()
        self._health_check_session = self._init_request_session(total_retries=1)

    @staticmethod
    def extract_id(uri: str) -> str:
        host = urlparse(uri).hostname

        if not host:
            raise RuntimeError(f"Could not parse the uri `{uri}`.")

        return host.split(".")[0].split("-")[0]

    @staticmethod
    def base_uri(aura_env: str | None = None) -> str:
        if aura_env is None or aura_env == "production":
            base_uri = "https://api.neo4j.io"
        elif aura_env == "staging":
            base_uri = "https://api-staging.neo4j.io"
        else:
            base_uri = f"https://api-{aura_env}.neo4j-dev.io"
        return base_uri

    def get_or_create_session(
        self,
        name: str,
        memory: SessionMemoryValue,
        instance_id: str | None = None,
        database_id: str | None = None,
        ttl: timedelta | None = None,
        cloud_location: CloudLocation | None = None,
    ) -> SessionDetails:
        json = {"name": name, "memory": memory.value, "project_id": self._project_id}

        if instance_id:
            json["instance_id"] = instance_id

        if database_id:
            json["database_id"] = database_id

        if ttl:
            json["ttl"] = f"{ttl.total_seconds()}s"

        if cloud_location:
            json["cloud_provider"] = cloud_location.provider
            json["region"] = cloud_location.region

        response = self._request_session.post(
            f"{self._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions", json=json
        )

        self._check_resp(response)

        raw_json: dict[str, Any] = response.json()
        self._check_errors(raw_json)

        return SessionDetails.from_json(raw_json["data"])

    def get_session(self, session_id: str) -> SessionDetails | None:
        response = self._request_session.get(
            f"{self._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/{session_id}"
        )

        if response.status_code == HTTPStatus.NOT_FOUND.value:
            return None

        self._check_resp(response)

        raw_json: dict[str, Any] = response.json()
        self._check_errors(raw_json)

        return SessionDetails.from_json(raw_json["data"])

    def get_session_with_errors(self, session_id: str) -> SessionDetailsWithErrors | None:
        """
        Same as `get_session`, but returns the session errors as part of the details instead of
        raising a `SessionStatusError`.
        """
        response = self._health_check_session.get(
            f"{self._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/{session_id}",
            timeout=AuraApi.HEALTH_CHECK_TIMEOUT,
        )

        if response.status_code == HTTPStatus.NOT_FOUND.value:
            return None

        self._check_resp(response)

        raw_json: dict[str, Any] = response.json()

        return SessionDetailsWithErrors.from_json_with_error(raw_json["data"], raw_json.get("errors", []))

    def list_sessions(
        self,
        instance_id: str | None = None,
        list_only_owned: bool = False,
        include_deleted: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[SessionDetailsWithErrors]:
        # these are query parameters (not passed in the body)
        params: dict[str, str] = {
            "projectId": self._project_id,
            "listOnlyOwned": str(list_only_owned).lower(),
            "includeDeleted": str(include_deleted).lower(),
        }

        if instance_id is not None:
            params["instanceId"] = instance_id

        if start_date is not None:
            params["startDate"] = start_date.isoformat()

        if end_date is not None:
            params["endDate"] = end_date.isoformat()

        response = self._request_session.get(
            f"{self._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions", params=params
        )

        self._check_resp(response)

        raw_json = response.json()

        data: list[Any] = raw_json.get("data", [])
        errors_per_session = defaultdict(list)
        for error in raw_json.get("errors", []):
            errors_per_session[error["id"]].append(error)

        return [SessionDetailsWithErrors.from_json_with_error(s, errors_per_session[s["id"]]) for s in data]

    def wait_for_session_running(
        self,
        session_id: str,
        sleep_time: float = 1.0,
        max_sleep_time: float = 10,
        max_wait_time: float = math.inf,
    ) -> WaitResult:
        waited_time = 0.0
        while waited_time < max_wait_time:
            session = self.get_session(session_id)
            if session is None:
                return WaitResult.from_error(f"Session `{session_id}` not found -- please retry")
            elif session.is_ready():
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
            f"Session `{session_id}` is not running after {waited_time} seconds.\n"
            "\tThe session may become available at a later time.\n"
            f'\tConsider running `sessions.delete(session_id="{session_id}")` '
            "to avoid resource leakage."
        )

    def delete_session(self, session_id: str) -> bool:
        response = self._request_session.delete(
            f"{self._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/{session_id}",
        )
        self._check_endpoint_deprecation(response)

        if response.status_code == HTTPStatus.NOT_FOUND.value:
            return False

        self._check_status_code(response)
        return response.status_code == HTTPStatus.ACCEPTED.value

    def create_instance(
        self, name: str, memory: SessionMemoryValue, cloud_provider: str, region: str, type: str = "dsenterprise"
    ) -> InstanceCreateDetails:
        data = {
            "name": name,
            "memory": memory.value,
            "version": "5",
            "region": region,
            "type": type,
            "tenant_id": self._project_id,
            "cloud_provider": cloud_provider,
        }

        response = self._request_session.post(f"{self._base_uri}/v1/instances", json=data)

        self._check_resp(response)

        return InstanceCreateDetails.from_json(response.json()["data"])

    def delete_instance(self, instance_id: str) -> InstanceSpecificDetails | None:
        # Delete an AuraDB instance.
        # If the instance cannot be found or was already deleted, returns None.
        response = self._request_session.delete(f"{self._base_uri}/v1/instances/{instance_id}")

        if response.status_code in [HTTPStatus.NOT_FOUND.value, HTTPStatus.GONE.value]:
            return None

        self._check_resp(response)

        return InstanceSpecificDetails.fromJson(response.json()["data"])

    def list_instances(self) -> list[InstanceDetails]:
        response = self._request_session.get(f"{self._base_uri}/v1/instances", params={"tenantId": self._project_id})

        self._check_resp(response)

        raw_data = response.json()["data"]

        return [InstanceDetails.fromJson(i) for i in raw_data]

    def list_instance(self, instance_id: str) -> InstanceSpecificDetails | None:
        response = self._request_session.get(f"{self._base_uri}/v1/instances/{instance_id}")

        if response.status_code == HTTPStatus.NOT_FOUND.value:
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
        self,
        node_count: int,
        node_label_count: int,
        node_property_count: int,
        relationship_count: int,
        relationship_property_count: int,
        algorithm_categories: list[AlgorithmCategory],
        algorithms: dict[str, dict[str, Any]] | None = None,
    ) -> EstimationDetails:
        data: dict[str, Any] = {
            "node_count": node_count,
            "node_label_count": node_label_count,
            "node_property_count": node_property_count,
            "relationship_count": relationship_count,
            "relationship_property_count": relationship_property_count,
        }

        # The API rejects `algorithm_categories` being present at all if `algorithms` is given, and vice versa.
        if algorithm_categories:
            data["algorithm_categories"] = [i.value for i in algorithm_categories]

        if algorithms:
            data["algorithms"] = [
                {"name": name, **({"config": config} if config else {})} for name, config in algorithms.items()
            ]

        response = self._request_session.post(
            f"{self._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/sizing", json=data
        )
        self._check_resp(response)

        return EstimationDetails.from_json(response.json()["data"])

    def _get_project_id(self) -> str:
        response = self._request_session.get(f"{self._base_uri}/v1/tenants")
        self._check_resp(response)

        raw_data = response.json()["data"]

        if len(raw_data) != 1:
            projects_dict = {d["id"]: d["name"] for d in raw_data}
            raise RuntimeError(
                f"This account has access to multiple projects: `{projects_dict}`. Please specify which one to use."
            )

        return raw_data[0]["id"]  # type: ignore

    def project_details(self) -> ProjectDetails:
        if not self._project_details:
            response = self._request_session.get(f"{self._base_uri}/v1/tenants/{self._project_id}")
            self._check_resp(response)
            self._project_details = ProjectDetails.from_json(response.json()["data"])
        return self._project_details

    def _check_errors(self, raw_json: dict[str, Any]) -> None:
        errors = raw_json.get("errors", [])
        typed_errors = [SessionErrorData.from_json(error) for error in errors] if errors else None

        if typed_errors:
            raise SessionStatusError(typed_errors)

    def _check_resp(self, resp: requests.Response) -> None:
        self._check_status_code(resp)
        self._check_endpoint_deprecation(resp)

    def _check_status_code(self, resp: requests.Response) -> None:
        if resp.status_code >= 400:
            message = ""
            try:
                message = resp.json()
            except requests.JSONDecodeError:
                try:
                    message = resp.text
                except Exception:
                    message = f"Not parsable body `{resp.raw.data!r}`"

            raise AuraApiError(
                f"Request for {resp.url} failed with status code {resp.status_code} - {resp.reason}: `{message}`",
                status_code=resp.status_code,
            )

    def _check_endpoint_deprecation(self, resp: requests.Response) -> None:
        expiry_date = resp.headers.get("X-Tyk-Api-Expires")
        if expiry_date:
            warnings.warn(
                f"The endpoint is deprecated and will be removed on {expiry_date}."
                " Please update to a newer version of this client.",
                DeprecationWarning,
            )

    class TokenRefreshSession(requests.Session):
        """Retries a request once with a fresh token if it was rejected as unauthorized.

        `Auth` signs a request once, when it is prepared, so any retry below this layer replays
        that same token. A request that is retried or stalls for longer than the token had left
        therefore arrives with a token the server rightfully rejects. The gateway may also reject
        a token the client still considers valid, if its own notion of the lifetime is shorter
        than the one advertised by `expires_in`.

        Without this, such a rejection is permanent for as long as the token stays cached, since
        `Auth` keeps replaying it until it is due for refresh.
        """

        def __init__(self, auth: AuraApi.Auth) -> None:
            super().__init__()
            self._auth = auth
            self._logger = logging.getLogger()

        def request(self, method: str | bytes, url: str | bytes, *args: Any, **kwargs: Any) -> requests.Response:
            kwargs.setdefault("timeout", AuraApi.DEFAULT_TIMEOUT)
            response = super().request(method, url, *args, **kwargs)

            if response.status_code != HTTPStatus.UNAUTHORIZED.value:
                return response

            self._logger.debug("Request was unauthorized, retrying it with a new oauth token")
            # A 401 means the request was rejected before being acted on, so resending it is
            # safe even for the methods we otherwise refuse to retry.
            response.close()
            self._auth.invalidate_token()

            return super().request(method, url, *args, **kwargs)

    class Auth(requests.auth.AuthBase):
        class Token:
            access_token: str
            expires_in: int
            token_type: str

            def __init__(self, json: dict[str, Any]) -> None:
                self.access_token = json["access_token"]
                self.token_type = json["token_type"]

                expires_in: int = json["expires_in"]
                refresh_in: int = expires_in if expires_in <= 10 else expires_in - 10
                # avoid token expiry during request send by refreshing 10 seconds earlier
                self.refresh_at = int(time.time()) + refresh_in

            def should_refresh(self) -> bool:
                return self.refresh_at <= int(time.time())

        def __init__(self, oauth_url: str, credentials: tuple[str, str], headers: dict[str, Any]) -> None:
            self._token: AuraApi.Auth.Token | None = None
            self._logger = logging.getLogger()
            self._oauth_url = oauth_url
            self._credentials = credentials
            self._headers = headers
            self._request_session = self._init_request_session(headers)

        def __getstate__(self) -> dict[str, Any]:
            state = self.__dict__.copy()
            # rebuilt on deserialization
            del state["_request_session"]
            return state

        def __setstate__(self, state: dict[str, Any]) -> None:
            self.__dict__.update(state)
            self._request_session = self._init_request_session(self._headers)

        def _init_request_session(self, headers: dict[str, Any]) -> requests.Session:
            request_session = requests.Session()
            request_session.mount(
                "https://",
                HTTPAdapter(
                    max_retries=Retry(
                        allowed_methods=["POST"],  # auth POST request is okay to retry
                        total=5,
                        status_forcelist=[
                            HTTPStatus.TOO_MANY_REQUESTS.value,
                            HTTPStatus.INTERNAL_SERVER_ERROR.value,
                            HTTPStatus.BAD_GATEWAY.value,
                            HTTPStatus.SERVICE_UNAVAILABLE.value,
                            HTTPStatus.GATEWAY_TIMEOUT.value,
                        ],
                        backoff_factor=0.1,
                    )
                ),
            )
            request_session.headers = headers
            return request_session

        def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
            r.headers["Authorization"] = f"Bearer {self._auth_token()}"
            return r

        def invalidate_token(self) -> None:
            self._token = None

        def _auth_token(self) -> str:
            if self._token is None or self._token.should_refresh():
                self._token = self._update_token()
            return self._token.access_token

        def _update_token(self) -> AuraApi.Auth.Token:
            data = {
                "grant_type": "client_credentials",
            }

            self._logger.debug("Updating oauth token")

            resp = self._request_session.post(
                self._oauth_url,
                data=data,
                auth=(self._credentials[0], self._credentials[1]),
                timeout=AuraApi.DEFAULT_TIMEOUT,
            )

            if resp.status_code >= 400:
                raise AuraApiError(
                    "Failed to authorize with provided client credentials: "
                    + f"{resp.status_code} - {resp.reason}, {resp.text}",
                    status_code=resp.status_code,
                )

            return AuraApi.Auth.Token(resp.json())
