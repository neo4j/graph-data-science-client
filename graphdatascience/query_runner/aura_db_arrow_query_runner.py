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
    config: Dict[str, Any]


class AuraDbArrowQueryRunner(QueryRunner):
    def __init__(
        self, fallback_query_runner: QueryRunner, aura_db_connection_info: Optional[AuraDbConnectionInfo] = None
    ):
        self._fallback_query_runner = fallback_query_runner
        self._aura_db_connection_info = aura_db_connection_info

    def run_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        if "gds.alpha.graph.project.remote" in query:
            token, aura_db_arrow_endpoint = self._get_or_request_auth_pair()
            params["token"] = token
            params["host"] = aura_db_arrow_endpoint
            # TODO: make this part of mandatory signature
            params["remote_database"] = database if database else self.database()

        self._fallback_query_runner.run_query(query, params, database, custom_error)

    def set_database(self, database: str) -> None:
        self._fallback_query_runner.set_database(database)

    def database(self) -> Optional[str]:
        return self._fallback_query_runner.database()

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        self._fallback_query_runner.create_graph_constructor(graph_name, concurrency, undirected_relationship_types)

    def close(self) -> None:
        self._fallback_qeury_runner.close()

    def _get_or_request_auth_pair(self) -> Tuple[str, str]:
        aura_db_endpoint, auth, config = self._aura_db_connection_info

        driver = GraphDatabase.driver(aura_db_endpoint, auth=auth, **config)
        query_runner = Neo4jQueryRunner(driver, auto_close=True)

        arrow_info: "Series[Any]" = query_runner.run_query(
            "CALL gds.debug.arrow.plugin()", custom_error=False
        ).squeeze()
        if not arrow_info.get("running"):
            raise RuntimeError("Arrow server is not running")
        listen_address: str = arrow_info.get("advertisedListenAddress")
        if not listen_address:
            raise ConnectionError("Did not retrieve connection info from database")

        host, port_string = listen_address.split(":")

        auth_pair_middleware = AuthPairInterceptingMiddleware()
        client_options: Dict[str, Any] = {
            "middleware": [AuthPairInterceptingMiddlewareFactory(auth_pair_middleware)],
            "disable_server_verification": True,
        }
        location = (
            flight.Location.for_grpc_tls(host, int(port_string))
            if driver.encrypted
            else flight.Location.for_grpc_tcp(host, int(port_string))
        )
        client = flight.FlightClient(location, **client_options)

        client.authenticate_basic_token(auth[0], auth[1])

        return (auth_pair_middleware.token(), auth_pair_middleware.endpoint())


class AuthPairInterceptingMiddlewareFactory(ClientMiddlewareFactory):
    def __init__(self, middleware: "AuthPairInterceptingMiddleware") -> None:
        self._middleware = middleware

    def start_call(self, info: any) -> "AuthPairInterceptingMiddleware":
        return self._middleware


class AuthPairInterceptingMiddleware(ClientMiddleware):
    def received_headers(self, headers: Dict[str, Any]) -> None:
        auth_header: str = headers.get("authorization", None)[0]
        if not auth_header:
            return
        [auth_type, token] = auth_header.split(" ", 1)
        if auth_type == "Bearer":
            self._token = token

        arrow_address_header: str = headers.get("arrowpluginaddress")[0]
        if arrow_address_header:
            self._arrow_address = arrow_address_header

    def sending_headers(self) -> Dict[str, str]:
        pass

    def token(self) -> str:
        return self._token

    def endpoint(self) -> str:
        return self._arrow_address
