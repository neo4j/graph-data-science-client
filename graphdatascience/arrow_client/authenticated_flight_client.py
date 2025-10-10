from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterator

from pyarrow import __version__ as arrow_version
from pyarrow import flight
from pyarrow._flight import (
    Action,
    ActionType,
    FlightInternalError,
    FlightStreamReader,
    FlightTimedOutError,
    FlightUnavailableError,
    Result,
    Ticket,
)
from tenacity import retry, retry_any, retry_if_exception_type, stop_after_attempt, stop_after_delay, wait_exponential

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.retry_utils.retry_config import RetryConfig

from ..retry_utils.retry_utils import before_log
from ..version import __version__
from .middleware.auth_middleware import AuthFactory, AuthMiddleware
from .middleware.user_agent_middleware import UserAgentFactory


class AuthenticatedArrowClient:
    @staticmethod
    def create(
        arrow_info: ArrowInfo,
        auth: ArrowAuthentication | None = None,
        encrypted: bool = False,
        arrow_client_options: dict[str, Any] | None = None,
        connection_string_override: str | None = None,
        retry_config: RetryConfig | None = None,
        advertised_listen_address: tuple[str, int] | None = None,
    ) -> AuthenticatedArrowClient:
        connection_string: str
        if connection_string_override is not None:
            connection_string = connection_string_override
        else:
            connection_string = arrow_info.listenAddress

        host, port = connection_string.split(":")

        if retry_config is None:
            retry_config = RetryConfig(
                retry=retry_any(
                    retry_if_exception_type(FlightTimedOutError),
                    retry_if_exception_type(FlightUnavailableError),
                    retry_if_exception_type(FlightInternalError),
                ),
                stop=(stop_after_delay(10) | stop_after_attempt(5)),
                wait=wait_exponential(multiplier=1, min=1, max=10),
            )

        return AuthenticatedArrowClient(
            host=host,
            retry_config=retry_config,
            port=int(port),
            auth=auth,
            encrypted=encrypted,
            arrow_client_options=arrow_client_options,
            advertised_listen_address=advertised_listen_address,
        )

    def __init__(
        self,
        host: str,
        retry_config: RetryConfig,
        port: int = 8491,
        auth: ArrowAuthentication | None = None,
        encrypted: bool = False,
        arrow_client_options: dict[str, Any] | None = None,
        user_agent: str | None = None,
        advertised_listen_address: tuple[str, int] | None = None,
    ):
        """Creates a new GdsArrowClient instance.

        Parameters
        ----------
        host: str
            The host address of the GDS Arrow server
        port: int
            The host port of the GDS Arrow server (default is 8491)
        auth: ArrowAuthentication | None
            An implementation of ArrowAuthentication providing a pair to be used for basic authentication
        encrypted: bool
            A flag that indicates whether the connection should be encrypted (default is False)
        arrow_client_options: dict[str, Any] | None
            Additional options to be passed to the Arrow Flight client.
        user_agent: str | None
            The user agent string to use for the connection. (default is `neo4j-graphdatascience-v[VERSION] pyarrow-v[PYARROW_VERSION])
        retry_config: RetryConfig | None
            The retry configuration to use for the Arrow requests send by the client.
        advertised_listen_address: tuple[str, int] | None
            The advertised listen address of the GDS Arrow server. This will be used by remote projection and writeback operations.
        """
        self._host = host
        self._port = port
        self._auth = None
        self._encrypted = encrypted
        self._arrow_client_options = arrow_client_options
        self._user_agent = user_agent
        self._retry_config = retry_config
        self._logger = logging.getLogger("gds_arrow_client")
        self._retry_config = retry_config
        if auth:
            self._auth = auth
            self._auth_middleware = AuthMiddleware(auth)
        self.advertised_listen_address = advertised_listen_address

        self._flight_client = self._instantiate_flight_client()

    def connection_info(self) -> ConnectionInfo:
        """
        Returns the host and port of the GDS Arrow server.

        Returns
        -------
        tuple[str, int]
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

        @retry(
            reraise=True,
            before=before_log("Request token", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def auth_with_retry() -> None:
            client = self._flight_client
            if self._auth:
                auth_pair = self._auth.auth_pair()
                client.authenticate_basic_token(auth_pair[0], auth_pair[1])

        if self._auth:
            auth_with_retry()
            return self._auth_middleware.token()
        else:
            return "IGNORED"

    def get_stream(self, ticket: Ticket) -> FlightStreamReader:
        return self._flight_client.do_get(ticket)

    def do_action(self, endpoint: str, payload: bytes | dict[str, Any]) -> Iterator[Result]:
        payload_bytes = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")

        return self._flight_client.do_action(Action(endpoint, payload_bytes))  # type: ignore

    def do_action_with_retry(self, endpoint: str, payload: bytes | dict[str, Any]) -> Iterator[Result]:
        @retry(
            reraise=True,
            before=before_log("Send action", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def run_with_retry() -> Iterator[Result]:
            return self.do_action(endpoint, payload)

        return run_with_retry()

    def list_actions(self) -> set[ActionType]:
        return self._flight_client.list_actions()  # type: ignore

    def _instantiate_flight_client(self) -> flight.FlightClient:
        location = (
            flight.Location.for_grpc_tls(self._host, self._port)
            if self._encrypted
            else flight.Location.for_grpc_tcp(self._host, self._port)
        )
        client_options: dict[str, Any] = (self._arrow_client_options or {}).copy()
        if self._auth:
            user_agent = f"neo4j-graphdatascience-v{__version__} pyarrow-v{arrow_version}"
            if self._user_agent:
                user_agent = self._user_agent

            client_options["middleware"] = [
                AuthFactory(self._auth_middleware),
                UserAgentFactory(useragent=user_agent),
            ]

        return flight.FlightClient(location, **client_options)


@dataclass
class ConnectionInfo:
    host: str
    port: int
    encrypted: bool
