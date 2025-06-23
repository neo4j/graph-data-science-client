from __future__ import annotations

import logging
from typing import Optional, Union, Any

from pyarrow import __version__ as arrow_version
from pyarrow import flight
from pyarrow._flight import FlightTimedOutError, FlightUnavailableError, FlightInternalError, Action
from tenacity import retry_any, retry_if_exception_type, stop_after_delay, stop_after_attempt, wait_exponential, retry

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.retry_utils.retry_config import RetryConfig
from .middleware.AuthMiddleware import AuthMiddleware, AuthFactory
from .middleware.UserAgentMiddleware import UserAgentFactory
from ..retry_utils.retry_utils import before_log
from ..version import __version__


class AuthenticatedArrowClient:

    @staticmethod
    def create(
            arrow_info: ArrowInfo,
            auth: Optional[ArrowAuthentication] = None,
            encrypted: bool = False,
            disable_server_verification: bool = False,
            tls_root_certs: Optional[bytes] = None,
            connection_string_override: Optional[str] = None,
            retry_config: Optional[RetryConfig] = None,
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
            host,
            retry_config,
            int(port),
            auth,
            encrypted,
            disable_server_verification,
            tls_root_certs,
        )

    def __init__(
            self,
            host: str,
            retry_config: RetryConfig,
            port: int = 8491,
            auth: Optional[Union[ArrowAuthentication, tuple[str, str]]] = None,
            encrypted: bool = False,
            disable_server_verification: bool = False,
            tls_root_certs: Optional[bytes] = None,
            user_agent: Optional[str] = None,
    ):
        """Creates a new GdsArrowClient instance.

        Parameters
        ----------
        host: str
            The host address of the GDS Arrow server
        port: int
            The host port of the GDS Arrow server (default is 8491)
        auth: Optional[Union[ArrowAuthentication, tuple[str, str]]]
            Either an implementation of ArrowAuthentication providing a pair to be used for basic authentication, or a username, password tuple
        encrypted: bool
            A flag that indicates whether the connection should be encrypted (default is False)
        disable_server_verification: bool
            A flag that disables server verification for TLS connections (default is False)
        tls_root_certs: Optional[bytes]
            PEM-encoded certificates that are used for the connection to the GDS Arrow Flight server
        arrow_endpoint_version:
            The version of the Arrow endpoint to use (default is ArrowEndpointVersion.V1)
        user_agent: Optional[str]
            The user agent string to use for the connection. (default is `neo4j-graphdatascience-v[VERSION] pyarrow-v[PYARROW_VERSION])
        retry_config: Optional[RetryConfig]
            The retry configuration to use for the Arrow requests send by the client.
        """
        self._host = host
        self._port = port
        self._auth = None
        self._encrypted = encrypted
        self._disable_server_verification = disable_server_verification
        self._tls_root_certs = tls_root_certs
        self._user_agent = user_agent
        self._retry_config = retry_config
        self._logger = logging.getLogger("gds_arrow_client")
        self._retry_config = RetryConfig(
            retry=retry_any(
                retry_if_exception_type(FlightTimedOutError),
                retry_if_exception_type(FlightUnavailableError),
                retry_if_exception_type(FlightInternalError),
            ),
            stop=(stop_after_delay(10) | stop_after_attempt(5)),
            wait=wait_exponential(multiplier=1, min=1, max=10),
        )

        if auth:
            self._auth = auth
            self._auth_middleware = AuthMiddleware(auth)

        self._flight_client = self._instantiate_flight_client()


    def do_action(self, endpoint: str, payload: bytes):
        return self._flight_client.do_action(Action(endpoint, payload))

    def do_action_with_retry(self, endpoint: str, payload: bytes):
        @retry(
            reraise=True,
            before=before_log("Send action", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def run_with_retry():
            return self.do_action(endpoint, payload)

        return run_with_retry()

    def _instantiate_flight_client(self) -> flight.FlightClient:
        location = (
            flight.Location.for_grpc_tls(self._host, self._port)
            if self._encrypted
            else flight.Location.for_grpc_tcp(self._host, self._port)
        )
        client_options: dict[str, Any] = {"disable_server_verification": self._disable_server_verification}
        if self._auth:
            user_agent = f"neo4j-graphdatascience-v{__version__} pyarrow-v{arrow_version}"
            if self._user_agent:
                user_agent = self._user_agent

            client_options["middleware"] = [
                AuthFactory(self._auth_middleware),
                UserAgentFactory(useragent=user_agent),
            ]
        if self._tls_root_certs:
            client_options["tls_root_certs"] = self._tls_root_certs
        return flight.FlightClient(location, **client_options)


