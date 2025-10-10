from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.graph_constructor import GraphConstructor
from graphdatascience.query_runner.query_mode import QueryMode


class StandaloneSessionQueryRunner(QueryRunner):
    def __init__(self, session_query_runner: QueryRunner):
        self._query_runner = session_query_runner

    def call_procedure(
        self,
        endpoint: str,
        params: CallParameters | None = None,
        yields: list[str] | None = None,
        database: str | None = None,
        mode: QueryMode = QueryMode.READ,
        logging: bool = False,
        retryable: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if endpoint.endswith(".write"):
            raise NotImplementedError("write procedures are not supported on standalone sessions")

        return self._query_runner.call_procedure(
            endpoint, params, yields, database, logging=logging, retryable=retryable, custom_error=custom_error
        )

    def call_function(self, endpoint: str, params: CallParameters | None = None) -> Any:
        return self._query_runner.call_function(endpoint, params)

    def server_version(self) -> ServerVersion:
        return self._query_runner.server_version()

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: list[str] | None
    ) -> GraphConstructor:
        return self._query_runner.create_graph_constructor(graph_name, concurrency, undirected_relationship_types)

    def set_show_progress(self, show_progress: bool) -> None:
        self._query_runner.set_show_progress(show_progress)

    def encrypted(self) -> bool:
        return self._query_runner.encrypted()

    def close(self) -> None:
        self._query_runner.close()

    def database(self) -> str | None:
        return "neo4j"

    def run_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        mode: QueryMode | None = None,
        custom_error: bool = True,
    ) -> DataFrame:
        raise NotImplementedError

    def run_retryable_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        mode: QueryMode | None = None,
        custom_error: bool = True,
    ) -> DataFrame:
        raise NotImplementedError

    def driver_config(self) -> dict[str, Any]:
        raise NotImplementedError

    def set_database(self, database: str) -> None:
        raise NotImplementedError

    def set_bookmarks(self, bookmarks: Any | None) -> None:
        raise NotImplementedError

    def bookmarks(self) -> Any | None:
        raise NotImplementedError

    def last_bookmarks(self) -> Any | None:
        raise NotImplementedError

    def set_server_version(self, _: ServerVersion) -> None:
        super().set_server_version(_)

    def cloneWithoutRouting(self, host: str, port: int) -> QueryRunner:
        return self
