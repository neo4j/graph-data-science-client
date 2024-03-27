from typing import Any, Dict, List, Optional, Tuple

from pandas import DataFrame, Series
from pyarrow import flight
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory

from .gds_arrow_client import GdsArrowClient
from ..call_parameters import CallParameters
from ..session.dbms_connection_info import DbmsConnectionInfo
from .query_runner import QueryRunner
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.server_version.server_version import ServerVersion


class AuraDbArrowQueryRunner(QueryRunner):
    GDS_REMOTE_PROJECTION_PROC_NAME = "gds.arrow.project"

    def __init__(
        self,
        gds_query_runner: QueryRunner,
        db_query_runner: QueryRunner,
        encrypted: bool,
        gds_connection_info: DbmsConnectionInfo,
    ):
        self._gds_query_runner = gds_query_runner
        self._db_query_runner = db_query_runner
        self._gds_connection_info = gds_connection_info

        self._gds_arrow_client = GdsArrowClient.create(
            gds_query_runner,
            auth=self._gds_connection_info.auth(),
            encrypted=encrypted
        )

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
            host, port = self._gds_arrow_client.connection_info()
            token = self._gds_arrow_client.get_or_request_token()
            params["arrowConfiguration"] = {
                "host": host,
                "port": port,
                "token": token,
            }

            return self._db_query_runner.call_procedure(endpoint, params, yields, database, logging, custom_error)

        elif ".write" in endpoint and self.is_remote_projected_graph(params["graph_name"]):
            raise "todo"
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
        self._gds_arrow_client.close()
        self._gds_query_runner.close()
        self._db_query_runner.close()

