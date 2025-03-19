from __future__ import annotations

import warnings
from typing import Any, Optional

from pandas import DataFrame

from graphdatascience.retry_utils.retry_config import RetryConfig

from ..call_parameters import CallParameters
from ..query_runner.arrow_info import ArrowInfo
from ..server_version.server_version import ServerVersion
from .arrow_graph_constructor import ArrowGraphConstructor
from .gds_arrow_client import GdsArrowClient
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class ArrowQueryRunner(QueryRunner):
    @staticmethod
    def create(
        fallback_query_runner: QueryRunner,
        arrow_info: ArrowInfo,
        auth: Optional[tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        connection_string_override: Optional[str] = None,
        retry_config: Optional[RetryConfig] = None,
    ) -> ArrowQueryRunner:
        if not arrow_info.enabled:
            raise ValueError("Arrow is not enabled on the server")

        gds_arrow_client = GdsArrowClient.create(
            arrow_info,
            auth,
            encrypted,
            disable_server_verification,
            tls_root_certs,
            connection_string_override,
            retry_config=retry_config,
        )

        return ArrowQueryRunner(gds_arrow_client, fallback_query_runner, fallback_query_runner.server_version())

    def __init__(
        self,
        gds_arrow_client: GdsArrowClient,
        fallback_query_runner: QueryRunner,
        server_version: ServerVersion,
    ):
        self._fallback_query_runner = fallback_query_runner
        self._gds_arrow_client = gds_arrow_client
        self._server_version = server_version

    def warn_about_deprecation(self, old_endpoint: str, new_endpoint: str) -> None:
        warnings.warn(
            DeprecationWarning(f"The endpoint '{old_endpoint}' is deprecated. Please use '{new_endpoint}' instead.")
        )

    def run_cypher(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        return self._fallback_query_runner.run_cypher(query, params, database, custom_error)

    def call_function(self, endpoint: str, params: Optional[CallParameters] = None) -> Any:
        return self._fallback_query_runner.call_function(endpoint, params)

    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[list[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        # We need to support the deprecated endpoints until they get removed on the server side
        if (
            old_endpoint := ("gds.graph.streamNodeProperty" == endpoint)
        ) or "gds.graph.nodeProperty.stream" == endpoint:
            if old_endpoint:
                self.warn_about_deprecation(
                    old_endpoint="gds.graph.streamNodeProperty", new_endpoint="gds.graph.nodeProperty.stream"
                )

            graph_name = params["graph_name"]
            properties = params["properties"]
            node_labels = params["entities"]
            list_node_labels = params["config"].get("listNodeLabels")
            concurrency = params["config"].get("concurrency")

            return self._gds_arrow_client.get_node_properties(
                graph_name, self._database_or_throw(), properties, node_labels, list_node_labels, concurrency
            )
        elif (
            old_endpoint := ("gds.graph.streamNodeProperties" == endpoint)
        ) or "gds.graph.nodeProperties.stream" == endpoint:
            if old_endpoint:
                self.warn_about_deprecation(
                    old_endpoint="gds.graph.streamNodeProperties", new_endpoint="gds.graph.nodeProperties.stream"
                )

            graph_name = params["graph_name"]
            properties = params["properties"]
            node_labels = params["entities"]
            list_node_labels = params["config"].get("listNodeLabels")
            concurrency = params["config"].get("concurrency")

            return self._gds_arrow_client.get_node_properties(
                graph_name, self._database_or_throw(), properties, node_labels, list_node_labels, concurrency
            )

        elif (
            old_endpoint := ("gds.graph.streamRelationshipProperty" == endpoint)
        ) or "gds.graph.relationshipProperty.stream" == endpoint:
            if old_endpoint:
                self.warn_about_deprecation(
                    old_endpoint="gds.graph.streamRelationshipProperty",
                    new_endpoint="gds.graph.relationshipProperty.stream",
                )

            graph_name = params["graph_name"]
            property_name = params["properties"]
            relationship_types = params["entities"]
            concurrency = params["config"].get("concurrency")

            return self._gds_arrow_client.get_relationship_properties(
                graph_name, self._database_or_throw(), property_name, relationship_types, concurrency
            )
        elif (
            old_endpoint := ("gds.graph.streamRelationshipProperties" == endpoint)
        ) or "gds.graph.relationshipProperties.stream" == endpoint:
            if old_endpoint:
                self.warn_about_deprecation(
                    old_endpoint="gds.graph.streamRelationshipProperties",
                    new_endpoint="gds.graph.relationshipProperties.stream",
                )

            graph_name = params["graph_name"]
            property_name = params["properties"]
            relationship_types = params["entities"]
            concurrency = params["config"].get("concurrency")

            return self._gds_arrow_client.get_relationship_properties(
                graph_name, self._database_or_throw(), property_name, relationship_types, concurrency
            )

        elif (
            old_endpoint := ("gds.beta.graph.relationships.stream" == endpoint)
        ) or "gds.graph.relationships.stream" == endpoint:
            if old_endpoint:
                self.warn_about_deprecation(
                    old_endpoint="gds.beta.graph.relationships.stream",
                    new_endpoint="gds.graph.relationships.stream",
                )

            graph_name = params["graph_name"]
            relationship_types = params["relationship_types"]
            concurrency = params["config"].get("concurrency")

            return self._gds_arrow_client.get_relationships(
                graph_name, self._database_or_throw(), relationship_types, concurrency
            )

        return self._fallback_query_runner.call_procedure(endpoint, params, yields, database, logging, custom_error)

    def server_version(self) -> ServerVersion:
        return self._fallback_query_runner.server_version()

    def driver_config(self) -> dict[str, Any]:
        return self._fallback_query_runner.driver_config()

    def encrypted(self) -> bool:
        return self._fallback_query_runner.encrypted()

    def set_database(self, database: str) -> None:
        self._fallback_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Optional[Any]) -> None:
        self._fallback_query_runner.set_bookmarks(bookmarks)

    def database(self) -> Optional[str]:
        return self._fallback_query_runner.database()

    def _database_or_throw(self) -> str:
        database = self.database()
        if not database:
            raise ValueError(
                "For this call you must have explicitly specified a valid Neo4j database to target, "
                "using `GraphDataScience.set_database`."
            )

        return database

    def bookmarks(self) -> Optional[Any]:
        return self._fallback_query_runner.bookmarks()

    def last_bookmarks(self) -> Optional[Any]:
        return self._fallback_query_runner.last_bookmarks()

    def close(self) -> None:
        self._fallback_query_runner.close()
        self._gds_arrow_client.close()

    def fallback_query_runner(self) -> QueryRunner:
        return self._fallback_query_runner

    def set_show_progress(self, show_progress: bool) -> None:
        self._fallback_query_runner.set_show_progress(show_progress)

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[list[str]]
    ) -> GraphConstructor:
        database = self._database_or_throw()

        return ArrowGraphConstructor(
            database,
            graph_name,
            self._gds_arrow_client,
            concurrency,
            undirected_relationship_types,
        )
