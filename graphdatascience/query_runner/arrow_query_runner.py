from __future__ import annotations

import warnings
from typing import Any, Dict, List, Optional, Tuple

from pandas import DataFrame

from ..call_parameters import CallParameters
from ..server_version.server_version import ServerVersion
from .arrow_graph_constructor import ArrowGraphConstructor
from .gds_arrow_client import GdsArrowClient
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner
from graphdatascience.server_version.compatible_with import (
    IncompatibleServerVersionError,
)


class ArrowQueryRunner(QueryRunner):
    @staticmethod
    def create(
        fallback_query_runner: QueryRunner,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        connection_string_override: Optional[str] = None,
    ) -> QueryRunner:
        if not GdsArrowClient.is_arrow_enabled(fallback_query_runner):
            return fallback_query_runner

        gds_arrow_client = GdsArrowClient.create(
            fallback_query_runner,
            auth,
            encrypted,
            disable_server_verification,
            tls_root_certs,
            connection_string_override,
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
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        return self._fallback_query_runner.run_cypher(query, params, database, custom_error)

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

        new_endpoint_server_version = ServerVersion(2, 2, 0)
        no_tier_in_namespace_server_version = ServerVersion(2, 5, 0)

        # We need to support the deprecated endpoints until they get removed on the server side
        if (
            old_endpoint := ("gds.graph.streamNodeProperty" == endpoint)
        ) or "gds.graph.nodeProperty.stream" == endpoint:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            node_labels = params["entities"]

            config = {"node_property": property_name, "node_labels": node_labels}

            if "listNodeLabels" in params["config"]:
                config["list_node_labels"] = params["config"]["listNodeLabels"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamNodeProperty"
            else:
                endpoint = "gds.graph.nodeProperty.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamNodeProperty", new_endpoint="gds.graph.nodeProperty.stream"
                    )

            return self._gds_arrow_client.get_property(self.database(), graph_name, endpoint, config)
        elif (
            old_endpoint := ("gds.graph.streamNodeProperties" == endpoint)
        ) or "gds.graph.nodeProperties.stream" == endpoint:
            graph_name = params["graph_name"]

            config = {"node_properties": params["properties"], "node_labels": params["entities"]}

            if "listNodeLabels" in params["config"]:
                config["list_node_labels"] = params["config"]["listNodeLabels"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamNodeProperties"
            else:
                endpoint = "gds.graph.nodeProperties.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamNodeProperties", new_endpoint="gds.graph.nodeProperties.stream"
                    )
            return self._gds_arrow_client.get_property(
                self.database(),
                graph_name,
                endpoint,
                config,
            )
        elif (
            old_endpoint := ("gds.graph.streamRelationshipProperty" == endpoint)
        ) or "gds.graph.relationshipProperty.stream" == endpoint:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            relationship_types = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamRelationshipProperty"
            else:
                endpoint = "gds.graph.relationshipProperty.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamRelationshipProperty",
                        new_endpoint="gds.graph.relationshipProperty.stream",
                    )
            return self._gds_arrow_client.get_property(
                self.database(),
                graph_name,
                endpoint,
                {"relationship_property": property_name, "relationship_types": relationship_types},
            )
        elif (
            old_endpoint := ("gds.graph.streamRelationshipProperties" == endpoint)
        ) or "gds.graph.relationshipProperties.stream" == endpoint:
            graph_name = params["graph_name"]
            property_names = params["properties"]
            relationship_types = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamRelationshipProperties"
            else:
                endpoint = "gds.graph.relationshipProperties.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamRelationshipProperties",
                        new_endpoint="gds.graph.relationshipProperties.stream",
                    )

            return self._gds_arrow_client.get_property(
                self.database(),
                graph_name,
                endpoint,
                {"relationship_properties": property_names, "relationship_types": relationship_types},
            )
        elif (
            old_endpoint := ("gds.beta.graph.relationships.stream" == endpoint)
        ) or "gds.graph.relationships.stream" == endpoint:
            graph_name = params["graph_name"]
            relationship_types = params["relationship_types"]

            if self._server_version < new_endpoint_server_version:
                raise IncompatibleServerVersionError(
                    f"The call gds.beta.graph.relationships.stream with parameters {params} via Arrow requires GDS "
                    f"server version >= 2.2.0. The current version is {self._server_version}"
                )
            else:
                if self._server_version < no_tier_in_namespace_server_version:
                    endpoint = "gds.beta.graph.relationships.stream"
                else:
                    endpoint = "gds.graph.relationships.stream"
                    if old_endpoint:
                        self.warn_about_deprecation(
                            old_endpoint="gds.beta.graph.relationships.stream",
                            new_endpoint="gds.graph.relationships.stream",
                        )

            return self._gds_arrow_client.get_property(
                self.database(), graph_name, endpoint, {"relationship_types": relationship_types}
            )

        return self._fallback_query_runner.call_procedure(endpoint, params, yields, database, logging, custom_error)

    def server_version(self) -> ServerVersion:
        return self._fallback_query_runner.server_version()

    def driver_config(self) -> Dict[str, Any]:
        return self._fallback_query_runner.driver_config()

    def encrypted(self) -> bool:
        return self._fallback_query_runner.encrypted()

    def set_database(self, database: str) -> None:
        self._fallback_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Optional[Any]) -> None:
        self._fallback_query_runner.set_bookmarks(bookmarks)

    def database(self) -> Optional[str]:
        return self._fallback_query_runner.database()

    def bookmarks(self) -> Optional[Any]:
        return self._fallback_query_runner.bookmarks()

    def last_bookmarks(self) -> Optional[Any]:
        return self._fallback_query_runner.last_bookmarks()

    def close(self) -> None:
        self._fallback_query_runner.close()
        self._gds_arrow_client.close()

    def fallback_query_runner(self) -> QueryRunner:
        return self._fallback_query_runner

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        database = self.database()
        if not database:
            raise ValueError(
                "For this call you must have explicitly specified a valid Neo4j database to target, "
                "using `GraphDataScience.set_database`."
            )

        return ArrowGraphConstructor(
            database,
            graph_name,
            self._gds_arrow_client,
            concurrency,
            undirected_relationship_types,
        )
