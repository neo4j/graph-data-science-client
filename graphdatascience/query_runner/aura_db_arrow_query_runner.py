from typing import Any, Dict, List, Optional, Tuple

from pandas import DataFrame, Series
from pyarrow import flight
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory

from ..call_parameters import CallParameters
from ..session.dbms_connection_info import DbmsConnectionInfo
from .query_runner import QueryRunner
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.server_version.server_version import ServerVersion


class AuraDbArrowQueryRunner(QueryRunner):
    GDS_REMOTE_PROJECTION_PROC_NAME = "gds.graph.project.remoteDb"

    def __init__(
        self,
        gds_query_runner: QueryRunner,
        db_query_runner: QueryRunner,
        encrypted: bool,
        aura_db_connection_info: DbmsConnectionInfo,
    ):
        self._gds_query_runner = gds_query_runner
        self._db_query_runner = db_query_runner
        self._auth = aura_db_connection_info.auth()

        arrow_info: "Series[Any]" = db_query_runner.call_procedure(
            endpoint="internal.arrow.status", custom_error=False
        ).squeeze()

        if not arrow_info.get("running"):
            raise RuntimeError(f"The Arrow Server is not running at `{aura_db_connection_info.uri}`")
        listen_address: Optional[str] = arrow_info.get("advertisedListenAddress")  # type: ignore
        if not listen_address:
            raise ConnectionError("Did not retrieve connection info from database")

        host, port_string = listen_address.split(":")

        self._auth_pair_middleware = AuthPairInterceptingMiddleware()
        client_options: Dict[str, Any] = {
            "middleware": [AuthPairInterceptingMiddlewareFactory(self._auth_pair_middleware)],
            "disable_server_verification": True,
        }

        self._encrypted = encrypted
        location = (
            flight.Location.for_grpc_tls(host, int(port_string))
            if self._encrypted
            else flight.Location.for_grpc_tcp(host, int(port_string))
        )
        self._client = flight.FlightClient(location, **client_options)

    def run_cypher(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        return self._db_query_runner.run_cypher(query, params, database, custom_error)

    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[List[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        if AuraDbArrowQueryRunner.GDS_REMOTE_PROJECTION_PROC_NAME == endpoint:
            token, aura_db_arrow_endpoint = self._get_or_request_auth_pair()
            params["token"] = token
            params["host"] = aura_db_arrow_endpoint
            params["config"]["useEncryption"] = self._encrypted

        elif ".write" in endpoint and self.is_remote_projected_graph(params["graph_name"]):
            token, aura_db_arrow_endpoint = self._get_or_request_auth_pair()
            host, port_string = aura_db_arrow_endpoint.split(":")
            params["config"]["arrowConnectionInfo"] = {
                "hostname": host,
                "port": int(port_string),
                "bearerToken": token,
                "useEncryption": self._encrypted,
            }

        return self._gds_query_runner.call_procedure(endpoint, params, yields, database, logging, custom_error)

    def is_remote_projected_graph(self, graph_name: str) -> bool:
        database_location: str = self._gds_query_runner.call_procedure(
            endpoint="gds.graph.list",
            yields=["databaseLocation"],
            params=CallParameters(graph_name=graph_name),
        ).squeeze()
        return database_location == "remote"

    def server_version(self) -> ServerVersion:
        return self._db_query_runner.server_version()

    def driver_config(self) -> Dict[str, Any]:
        return self._db_query_runner.driver_config()

    def encrypted(self) -> bool:
        return self._db_query_runner.encrypted()

    def set_database(self, database: str) -> None:
        self._db_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Optional[Any]) -> None:
        self._db_query_runner.set_bookmarks(bookmarks)

    def bookmarks(self) -> Optional[Any]:
        return self._db_query_runner.bookmarks()

    def last_bookmarks(self) -> Optional[Any]:
        return self._db_query_runner.last_bookmarks()

    def database(self) -> Optional[str]:
        return self._db_query_runner.database()

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        return self._gds_query_runner.create_graph_constructor(graph_name, concurrency, undirected_relationship_types)

    def close(self) -> None:
        self._client.close()
        self._gds_query_runner.close()
        self._db_query_runner.close()

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
