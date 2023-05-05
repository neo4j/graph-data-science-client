from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from neo4j import GraphDatabase
from pandas import DataFrame, Series
from pyarrow import flight
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory

from .query_runner import QueryRunner
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


class AuraDbConnectionInfo(NamedTuple):
    uri: str
    auth: Tuple[str, str]


class AuraDbArrowQueryRunner(QueryRunner):
    def __init__(self, fallback_query_runner: QueryRunner, aura_db_connection_info: AuraDbConnectionInfo):
        self._fallback_query_runner = fallback_query_runner

        aura_db_endpoint, auth = aura_db_connection_info
        self._auth = auth

        config: Dict[str, Any] = {"max_connection_lifetime": 60}
        self._driver = GraphDatabase.driver(aura_db_endpoint, auth=auth, **config)
        arrow_info: "Series[Any]" = (
            Neo4jQueryRunner(self._driver, auto_close=True)
            .run_query("CALL gds.debug.arrow.plugin()", custom_error=False)
            .squeeze()
        )

        if not arrow_info.get("running"):
            raise RuntimeError("The plugin arrow server for AuraDB is not running")
        listen_address: Optional[str] = arrow_info.get("advertisedListenAddress")  # type: ignore
        if not listen_address:
            raise ConnectionError("Did not retrieve connection info from database")

        host, port_string = listen_address.split(":")

        self._auth_pair_middleware = AuthPairInterceptingMiddleware()
        client_options: Dict[str, Any] = {
            "middleware": [AuthPairInterceptingMiddlewareFactory(self._auth_pair_middleware)],
            "disable_server_verification": True,
        }
        location = (
            flight.Location.for_grpc_tls(host, int(port_string))
            if self._driver.encrypted
            else flight.Location.for_grpc_tcp(host, int(port_string))
        )
        self._client = flight.FlightClient(location, **client_options)

    def run_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = {}

        if "gds.alpha.graph.project.remote" in query:
            token, aura_db_arrow_endpoint = self._get_or_request_auth_pair()
            params["token"] = token
            params["host"] = aura_db_arrow_endpoint

        return self._fallback_query_runner.run_query(query, params, database, custom_error)

    def set_database(self, database: str) -> None:
        self._fallback_query_runner.set_database(database)

    def database(self) -> Optional[str]:
        return self._fallback_query_runner.database()

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        return self._fallback_query_runner.create_graph_constructor(
            graph_name, concurrency, undirected_relationship_types
        )

    def close(self) -> None:
        self._client.close()
        self._driver.close()
        self._fallback_query_runner.close()

    def _get_or_request_auth_pair(self) -> Tuple[str, str]:
        self._client.authenticate_basic_token(self._auth[0], self._auth[1])
        return (self._auth_pair_middleware.token(), self._auth_pair_middleware.endpoint())


class AuthPairInterceptingMiddlewareFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, middleware: "AuthPairInterceptingMiddleware", *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._middleware = middleware

    def start_call(self, info: Any) -> "AuthPairInterceptingMiddleware":
        return self._middleware


class AuthPairInterceptingMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def received_headers(self, headers: Dict[str, Any]) -> None:
        auth_header = headers.get("authorization")
        auth_type, token = self._read_auth_header(auth_header)
        if auth_type == "Bearer":
            self._token = token

        self._arrow_address = self._read_address_header(headers.get("arrowpluginaddress"))

    def sending_headers(self) -> Dict[str, str]:
        return {}

    def token(self) -> str:
        return self._token

    def endpoint(self) -> str:
        return self._arrow_address

    def _read_auth_header(self, auth_header: Any) -> Tuple[str, str]:
        if isinstance(auth_header, List):
            auth_header = auth_header[0]
        elif not isinstance(auth_header, str):
            raise ValueError("Incompatible header format '{}'", auth_header)

        auth_type, token = auth_header.split(" ", 1)
        return (str(auth_type), str(token))

    def _read_address_header(self, address_header: Any) -> str:
        if isinstance(address_header, List):
            return str(address_header[0])
        if isinstance(address_header, str):
            return address_header
        raise ValueError("Incompatible header format '{}'", address_header)
