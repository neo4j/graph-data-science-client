from __future__ import annotations

import json
import logging
import platform
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Callable, Iterator, Type, TypeVar

import certifi
from pyarrow import Schema, flight
from pyarrow import __version__ as arrow_version
from pyarrow.flight import (
    Action,
    ActionType,
    FlightCallOptions,
    FlightInternalError,
    FlightStreamReader,
    FlightTimedOutError,
    FlightUnavailableError,
    Result,
    Ticket,
)

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.server_health_check import ServerHealthCheck
from graphdatascience.retry_utils.retry_config import ExponentialWaitConfig, RetryConfigV2, StopConfig

from ..version import __version__
from .arrow_client_options_util import TLS_ROOT_CERTS_OPTION, set_tls_root_certs
from .middleware.auth_middleware import AuthFactory, AuthMiddleware
from .middleware.user_agent_middleware import UserAgentFactory

T = TypeVar("T")


class AuthenticatedArrowClient:
    """Arrow Flight client used to communicate with the GDS Arrow server."""

    def __init__(
        self,
        connection_info: str | tuple[str, int],
        auth: ArrowAuthentication | None = None,
        encrypted: bool = False,
        arrow_client_options: dict[str, Any] | None = None,
        user_agent: str | None = None,
        advertised_listen_address: tuple[str, int] | None = None,
        retry_config: RetryConfigV2 | None = None,
        health_check: ServerHealthCheck | None = None,
    ):
        """Creates a new AuthenticatedArrowClient instance.

        Parameters
        ----------
        connection_info
            The host address and port of the GDS Arrow server
        auth
            An implementation of ArrowAuthentication providing a pair to be used for basic authentication
        encrypted
            A flag that indicates whether the connection should be encrypted (default is False)
        arrow_client_options
            Additional options for the Arrow Flight client. The key ``call_timeout`` sets the
            per-call RPC timeout in seconds (default 30s); all other keys are forwarded to
            ``pyarrow.flight.FlightClient``.
        user_agent
            The user agent string to use for the connection. (default is `neo4j-graphdatascience-v[VERSION] pyarrow-v[PYARROW_VERSION]`)
        retry_config
            The retry configuration to use for the Arrow requests send by the client.
        advertised_listen_address
            The advertised listen address of the GDS Arrow server. This will be used by remote projection and writeback operations.
        health_check
            Optional health check consulted when the server could not be reached, after all retries were
            exhausted. It is expected to raise a more descriptive error if it can explain the failure (such as out-of-memory error) and to return without raising otherwise.
        """

        if isinstance(connection_info, str):
            host, port_str = connection_info.split(":")
            port = int(port_str)
        else:
            host, port = connection_info

        if retry_config is None:
            retry_config = RetryConfigV2(
                retryable_exceptions=[
                    FlightTimedOutError,
                    FlightUnavailableError,
                    FlightInternalError,
                ],
                stop_config=StopConfig(after_delay=10, after_attempt=5),
                wait_config=ExponentialWaitConfig(multiplier=1, min=1, max=10),
            )

        options_copy = dict(arrow_client_options) if arrow_client_options else {}
        call_timeout = options_copy.pop("call_timeout", 30.0)

        self._host = host
        self._port = int(port)
        self._auth = auth
        self._encrypted = encrypted
        self._arrow_client_options = options_copy or None
        self._user_agent = user_agent
        self._logger = logging.getLogger("gds_arrow_client")
        self._retry_config = retry_config
        self._health_check = health_check
        if auth:
            self._auth_middleware = AuthMiddleware(auth)
        self._call_timeout = call_timeout
        self._call_options = self._build_call_options()
        self.advertised_listen_address = advertised_listen_address
        self._flight_client: flight.FlightClient = self._instantiate_flight_client()

    def connection_info(self) -> ConnectionInfo:
        """
        Returns the host and port of the GDS Arrow server.

        Returns
        -------
        ConnectionInfo
            the host and port of the GDS Arrow server
        """
        return ConnectionInfo(self._host, self._port, self._encrypted)

    def advertised_connection_info(self) -> ConnectionInfo:
        """
        Returns the advertised host and port of the GDS Arrow server.

        Returns
        -------
        ConnectionInfo
            the host and port of the GDS Arrow server
        """
        if self.advertised_listen_address is None:
            return self.connection_info()

        h, p = self.advertised_listen_address
        return ConnectionInfo(h, p, self._encrypted)

    def request_token(self) -> str | None:
        """
        Requests a token from the server and returns it.

        Returns
        -------
        str | None
            a token from the server and returns it.
        """

        @self._retry_config.decorator(operation_name="Request token", logger=self._logger)
        def auth_with_retry() -> None:
            try:
                client = self._flight_client
                if self._auth:
                    auth_pair = self._auth.auth_pair()
                    client.authenticate_basic_token(auth_pair[0], auth_pair[1], self._call_options)
            except (FlightTimedOutError, FlightUnavailableError, FlightInternalError):
                self._reconnect()
                raise

        if self._auth:
            self._diagnose_connection_failure(auth_with_retry)
            return self._auth_middleware.token()
        else:
            return "IGNORED"

    def get_stream(self, ticket: Ticket) -> FlightStreamReader:
        return self._flight_client.do_get(ticket)

    def do_action(self, endpoint: str, payload: bytes | dict[str, Any]) -> Iterator[Result]:
        payload_bytes = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")

        return self._flight_client.do_action(Action(endpoint, payload_bytes), self._call_options)  # type: ignore

    def do_action_with_retry(self, endpoint: str, payload: bytes | dict[str, Any]) -> list[Result]:
        @self._retry_config.decorator(operation_name="Send action", logger=self._logger)
        def run_with_retry() -> list[Result]:
            try:
                # the Flight response error code is only checked on iterator consumption
                # we eagerly collect iterator here to trigger retry in case of an error
                return list(self.do_action(endpoint, payload))
            except (FlightTimedOutError, FlightUnavailableError, FlightInternalError):
                self._reconnect()
                raise

        return self._diagnose_connection_failure(run_with_retry)

    def list_actions(self) -> set[ActionType]:
        return self._flight_client.list_actions(self._call_options)  # type: ignore

    def list_actions_with_retry(self) -> set[ActionType]:
        @self._retry_config.decorator(operation_name="List actions", logger=self._logger)
        def run_with_retry() -> set[ActionType]:
            try:
                return self.list_actions()
            except (FlightTimedOutError, FlightUnavailableError, FlightInternalError):
                self._reconnect()
                raise

        return self._diagnose_connection_failure(run_with_retry)

    def do_put_with_retry(
        self, descriptor: flight.FlightDescriptor, schema: Schema
    ) -> tuple[flight.FlightStreamWriter, flight.FlightMetadataReader]:
        @self._retry_config.decorator(operation_name="Do put", logger=self._logger)
        def run_with_retry() -> tuple[flight.FlightStreamWriter, flight.FlightMetadataReader]:
            try:
                return self._flight_client.do_put(descriptor, schema)  # type: ignore
            except (FlightTimedOutError, FlightUnavailableError, FlightInternalError):
                self._reconnect()
                raise

        return self._diagnose_connection_failure(run_with_retry)

    def _diagnose_connection_failure(self, operation: Callable[[], T]) -> T:
        """
        Runs the given operation and, if the server could not be reached, gives the health check
        the chance to raise a more descriptive error.
        """
        try:
            return operation()
        except (FlightTimedOutError, FlightUnavailableError, FlightInternalError):
            if self._health_check:
                self._health_check.raise_if_unhealthy()
            raise

    def __enter__(self) -> AuthenticatedArrowClient:
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._flight_client:
            self._flight_client.close()

    def _build_call_options(self) -> FlightCallOptions | None:
        return FlightCallOptions(timeout=self._call_timeout) if self._call_timeout is not None else None

    def _instantiate_flight_client(self) -> flight.FlightClient:
        location = (
            flight.Location.for_grpc_tls(self._host, self._port)
            if self._encrypted
            else flight.Location.for_grpc_tcp(self._host, self._port)
        )
        client_options: dict[str, Any] = (self._arrow_client_options or {}).copy()

        # We need to specify the system root certificates on Windows
        if platform.system() == "Windows":
            if TLS_ROOT_CERTS_OPTION not in client_options:
                set_tls_root_certs(client_options, certifi.contents())

        if self._auth:
            user_agent = f"neo4j-graphdatascience-v{__version__} pyarrow-v{arrow_version}"
            if self._user_agent:
                user_agent = self._user_agent

            client_options["middleware"] = [
                AuthFactory(self._auth_middleware),
                UserAgentFactory(useragent=user_agent),
            ]

        return flight.FlightClient(location, **client_options)

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        # Remove the FlightClient as it isn't serializable
        if "_flight_client" in state:
            del state["_flight_client"]
        # FlightCallOptions is also not serializable
        if "_call_options" in state:
            del state["_call_options"]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self.__dict__.setdefault("_health_check", None)
        self.__dict__.setdefault("_call_timeout", 30.0)
        self.__dict__.setdefault("_call_options", self._build_call_options())
        self._flight_client = self._instantiate_flight_client()

    def _reconnect(self) -> None:
        try:
            self._flight_client.close()
        except Exception:
            pass

        self._flight_client = self._instantiate_flight_client()


@dataclass
class ConnectionInfo:
    """Host, port and encryption details for an Arrow server connection."""

    host: str
    port: int
    encrypted: bool
