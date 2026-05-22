from __future__ import annotations

from typing import Any
from uuid import uuid4

from pandas import DataFrame

from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.write_job_handle import WriteJobHandle
from graphdatascience.query_runner import QueryMode, QueryType
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.query_runner.progress.query_progress_logger import QueryProgressLogger
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver


class SessionQueryRunner(QueryRunner):
    GDS_REMOTE_PROJECTION_PROC_NAME = "gds.arrow.project"

    @staticmethod
    def create(
        gds_query_runner: QueryRunner,
        db_query_runner: QueryRunner,
        arrow_client: GdsArrowClient,
        show_progress: bool,
    ) -> SessionQueryRunner:
        return SessionQueryRunner(gds_query_runner, db_query_runner, arrow_client, show_progress)

    def __init__(
        self,
        gds_query_runner: QueryRunner,
        db_query_runner: QueryRunner,
        arrow_client: GdsArrowClient,
        show_progress: bool,
    ):
        self._gds_query_runner = gds_query_runner
        self._db_query_runner = db_query_runner
        self._gds_arrow_client = arrow_client
        self._resolved_protocol_version = ProtocolVersionResolver(db_query_runner).resolve()
        self._show_progress = show_progress
        self._progress_logger = QueryProgressLogger(
            lambda query, database: self._gds_query_runner.run_cypher(
                query=query, query_type=QueryType.USER_TRANSPILED, database=database
            ),
            self._gds_query_runner.server_version,
        )

    def run_cypher(
        self,
        query: str,
        query_type: QueryType,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        mode: QueryMode | None = None,
        custom_error: bool = True,
    ) -> DataFrame:
        return self._db_query_runner.run_cypher(query, query_type, params, database, mode, custom_error)

    def run_retryable_cypher(
        self,
        query: str,
        query_type: QueryType,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        mode: QueryMode | None = None,
        custom_error: bool = True,
    ) -> DataFrame:
        return self._db_query_runner.run_retryable_cypher(
            query, query_type, params, database, mode=mode, custom_error=custom_error
        )

    def call_function(
        self, endpoint: str, query_type: QueryType = QueryType.USER_TRANSPILED, params: CallParameters | None = None
    ) -> Any:
        return self._gds_query_runner.call_function(endpoint, query_type, params)

    def call_procedure(
        self,
        endpoint: str,
        query_type: QueryType = QueryType.USER_TRANSPILED,
        params: CallParameters | None = None,
        yields: list[str] | None = None,
        database: str | None = None,
        mode: QueryMode = QueryMode.READ,
        logging: bool = False,
        retryable: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        elif endpoint.endswith(".write") and self.is_remote_projected_graph(params["graph_name"]):
            termination_flag = TerminationFlag.create()
            return self._remote_write_back(endpoint, params, termination_flag, yields, database, logging, custom_error)

        return self._gds_query_runner.call_procedure(
            endpoint,
            query_type,
            params,
            yields,
            database,
            mode=mode,
            logging=logging,
            retryable=retryable,
            custom_error=custom_error,
        )

    def is_remote_projected_graph(self, graph_name: str) -> bool:
        database_location: str = self._gds_query_runner.call_procedure(
            endpoint="gds.graph.list",
            yields=["databaseLocation"],
            params=CallParameters(graph_name=graph_name),
        ).squeeze()
        return database_location == "remote"

    def server_version(self) -> ServerVersion:
        return self._db_query_runner.server_version()

    def driver_config(self) -> dict[str, Any]:
        return self._db_query_runner.driver_config()

    def encrypted(self) -> bool:
        return self._db_query_runner.encrypted()

    def set_database(self, database: str) -> None:
        self._db_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Any | None) -> None:
        self._db_query_runner.set_bookmarks(bookmarks)

    def bookmarks(self) -> Any | None:
        return self._db_query_runner.bookmarks()

    def last_bookmarks(self) -> Any | None:
        return self._db_query_runner.last_bookmarks()

    def database(self) -> str | None:
        return self._db_query_runner.database()

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: list[str] | None
    ) -> GraphConstructor:
        return self._gds_query_runner.create_graph_constructor(graph_name, concurrency, undirected_relationship_types)

    def set_show_progress(self, show_progress: bool) -> None:
        self._show_progress = show_progress
        self._gds_query_runner.set_show_progress(show_progress)

    def cloneWithoutRouting(self, host: str, port: int) -> QueryRunner:
        return SessionQueryRunner(
            self._gds_query_runner,
            self._db_query_runner.cloneWithoutRouting(host, port),
            self._gds_arrow_client,
            self._show_progress,
        )

    def close(self) -> None:
        self._gds_arrow_client.close()
        self._gds_query_runner.close()
        self._db_query_runner.close()

    def _remote_write_back(
        self,
        endpoint: str,
        params: CallParameters,
        terminationFlag: TerminationFlag,
        yields: list[str] | None = None,
        database: str | None = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params["config"] is None:
            params["config"] = {}

        config: dict[str, Any] = params.get("config", {})

        # remove so that it isn't retained for the GDS proc call below; the write protocol builds its own arrow config
        config.pop("arrowConfiguration", None)

        job_id = config["jobId"] if "jobId" in config else str(uuid4())
        config["jobId"] = job_id

        config["writeToResultStore"] = True

        gds_write_result = self._gds_query_runner.call_procedure(
            endpoint,
            QueryType.USER_TRANSPILED,
            params,
            yields,
            database=database,
            logging=logging,
            custom_error=custom_error,
        )
        terminationFlag.assert_running()

        graph_name = params["graph_name"]

        write_protocol = WriteProtocol.select(self._gds_arrow_client.flight_client(), self._db_query_runner)

        write_handle = WriteJobHandle.create(
            write_protocol, graph_name, job_id, terminationFlag, concurrency=config.get("concurrency")
        )
        database_write_result = write_handle.result(wait=True)

        gds_write_result["writeMillis"] = database_write_result.write_millis

        if "nodePropertiesWritten" in gds_write_result:
            gds_write_result["nodePropertiesWritten"] = database_write_result.written_node_properties
        if "propertiesWritten" in gds_write_result:
            gds_write_result["propertiesWritten"] = database_write_result.written_node_properties
        if "nodeLabelsWritten" in gds_write_result:
            gds_write_result["nodeLabelsWritten"] = database_write_result.written_node_labels
        if "relationshipsWritten" in gds_write_result:
            gds_write_result["relationshipsWritten"] = database_write_result.written_node_properties

        return gds_write_result

    def _resolve_show_progress(self, show_progress: bool) -> bool:
        return self._show_progress and show_progress
