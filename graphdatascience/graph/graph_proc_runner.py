from importlib.resources import path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from multimethod import multimethod
from pandas import DataFrame, Series, read_pickle

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_export_runner import GraphExportRunner
from .graph_object import Graph
from .graph_project_runner import GraphProjectRunner
from graphdatascience.graph.graph_alpha_proc_runner import GraphAlphaProcRunner
from graphdatascience.graph.graph_entity_ops_runner import (
    GraphElementPropertyRunner,
    GraphNodePropertiesRunner,
    GraphRelationshipPropertiesRunner,
    GraphRelationshipRunner,
    GraphRelationshipsRunner,
)
from graphdatascience.graph.graph_type_check import (
    graph_type_check,
    graph_type_check_optional,
)

Strings = Union[str, List[str]]


class GraphProcRunner(UncallableNamespace, IllegalAttrChecker):
    @client_only_endpoint("gds.graph")
    def load_cora(self, graph_name: str = "cora", undirected: bool = False) -> Graph:
        with path("graphdatascience.resources.cora", "cora_nodes_gzip.pkl") as nodes_resource:
            nodes = read_pickle(nodes_resource, compression="gzip")

        with path("graphdatascience.resources.cora", "cora_rels_gzip.pkl") as rels_resource:
            rels = read_pickle(rels_resource, compression="gzip")

        self._namespace = "gds.alpha.graph"
        alpha_proc_runner = GraphAlphaProcRunner(self._query_runner, self._namespace, self._server_version)

        undirected_relationship_types = ["*"] if undirected else []

        return alpha_proc_runner.construct(
            graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types
        )

    @client_only_endpoint("gds.graph")
    def load_karate_club(self, graph_name: str = "karate_club", undirected: bool = False) -> Graph:
        nodes = pd.DataFrame({"nodeId": range(1, 35)})
        nodes["labels"] = "Person"

        with path("graphdatascience.resources.karate", "karate_club_gzip.pkl") as rels_resource:
            rels = read_pickle(rels_resource, compression="gzip")

        self._namespace = "gds.alpha.graph"
        alpha_proc_runner = GraphAlphaProcRunner(self._query_runner, self._namespace, self._server_version)

        undirected_relationship_types = ["*"] if undirected else []

        return alpha_proc_runner.construct(
            graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types
        )

    @client_only_endpoint("gds.graph")
    def load_imdb(self, graph_name: str = "imdb", undirected: bool = True) -> Graph:
        if self._server_version < ServerVersion(2, 3, 0):
            raise ValueError("The IMDB dataset loading is only supported by GDS 2.3 or later.")

        with path("graphdatascience.resources.imdb", "imdb_movies_with_genre_gzip.pkl") as nodes_resource:
            movies_with_genre = read_pickle(nodes_resource, compression="gzip")
        with path("graphdatascience.resources.imdb", "imdb_movies_without_genre_gzip.pkl") as nodes_resource:
            movies_without_genre = read_pickle(nodes_resource, compression="gzip")
        with path("graphdatascience.resources.imdb", "imdb_actors_gzip.pkl") as nodes_resource:
            actors = read_pickle(nodes_resource, compression="gzip")
        with path("graphdatascience.resources.imdb", "imdb_directors_gzip.pkl") as nodes_resource:
            directors = read_pickle(nodes_resource, compression="gzip")

        with path("graphdatascience.resources.imdb", "imdb_acted_in_rels_gzip.pkl") as rels_resource:
            acted_in_rels = read_pickle(rels_resource, compression="gzip")
        with path("graphdatascience.resources.imdb", "imdb_directed_in_rels_gzip.pkl") as rels_resource:
            directed_in_rels = read_pickle(rels_resource, compression="gzip")

        self._namespace = "gds.alpha.graph"
        alpha_proc_runner = GraphAlphaProcRunner(self._query_runner, self._namespace, self._server_version)

        nodes = [movies_with_genre, movies_without_genre, actors, directors]
        rels = [acted_in_rels, directed_in_rels]

        # Default undirected which matches raw data
        undirected_relationship_types = ["*"] if undirected else []

        return alpha_proc_runner.construct(
            graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types
        )

    @property
    def project(self) -> GraphProjectRunner:
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def export(self) -> GraphExportRunner:
        self._namespace += ".export"
        return GraphExportRunner(self._query_runner, self._namespace, self._server_version)

    @graph_type_check
    def drop(
        self,
        G: Graph,
        failIfMissing: bool = False,
        dbName: str = "",
        username: Optional[str] = None,
    ) -> Optional["Series[Any]"]:
        self._namespace += ".drop"

        params = {
            "graph_name": G.name(),
            "fail_if_missing": failIfMissing,
            "db_name": dbName,
        }
        if username:
            query = f"CALL {self._namespace}($graph_name, $fail_if_missing, $db_name, $username)"
            params["username"] = username
        else:
            query = f"CALL {self._namespace}($graph_name, $fail_if_missing, $db_name)"

        result = self._query_runner.run_query(query, params)
        if not result.empty:
            return result.squeeze()  # type: ignore

        return None

    def exists(self, graph_name: str) -> "Series[Any]":
        self._namespace += ".exists"
        result = self._query_runner.run_query(f"CALL {self._namespace}($graph_name)", {"graph_name": graph_name})

        return result.squeeze()  # type: ignore

    @graph_type_check_optional
    def list(self, G: Optional[Graph] = None) -> DataFrame:
        self._namespace += ".list"

        if G:
            query = f"CALL {self._namespace}($graph_name)"
            params = {"graph_name": G.name()}
        else:
            query = "CALL gds.graph.list()"
            params = {}

        return self._query_runner.run_query(query, params)

    @client_only_endpoint("gds.graph")
    def get(self, graph_name: str) -> Graph:
        result = self._query_runner.run_query(f"CALL gds.graph.list('{graph_name}') YIELD graphName")
        if len(result["graphName"]) == 0:
            raise ValueError(
                f"No projected graph named '{graph_name}' exists in current database '{self._query_runner.database()}'"
            )

        return Graph(graph_name, self._query_runner, self._server_version)

    @graph_type_check
    def _handle_properties(
        self,
        G: Graph,
        properties: Strings,
        entities: Strings,
        config: Dict[str, Any],
    ) -> DataFrame:
        query = f"CALL {self._namespace}($graph_name, $properties, $entities, $config)"
        params = {
            "graph_name": G.name(),
            "properties": properties,
            "entities": entities,
            "config": config,
        }

        return self._query_runner.run_query(query, params)

    @property
    def nodeProperty(self) -> GraphElementPropertyRunner:
        self._namespace += ".nodeProperty"
        return GraphElementPropertyRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def nodeProperties(self) -> GraphNodePropertiesRunner:
        self._namespace += ".nodeProperties"
        return GraphNodePropertiesRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def relationshipProperty(self) -> GraphElementPropertyRunner:
        self._namespace += ".relationshipProperty"
        return GraphElementPropertyRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def relationshipProperties(self) -> GraphRelationshipPropertiesRunner:
        self._namespace += ".relationshipProperties"
        return GraphRelationshipPropertiesRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def relationship(self) -> GraphRelationshipRunner:
        self._namespace += ".relationship"
        return GraphRelationshipRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def relationships(self) -> GraphRelationshipsRunner:
        self._namespace += ".relationships"
        return GraphRelationshipsRunner(self._query_runner, self._namespace, self._server_version)

    def streamNodeProperties(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        separate_property_columns: bool = False,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".streamNodeProperties"

        result = self._handle_properties(G, node_properties, node_labels, config)

        # new format was requested, but the query was run via Cypher
        if separate_property_columns and "propertyValue" in result.keys():
            result = result.pivot(index="nodeId", columns="nodeProperty", values="propertyValue")
            result = result.reset_index()
            result.columns.name = None
        # old format was requested but the query was run via Arrow
        elif not separate_property_columns and "propertyValue" not in result.keys():
            result = result.melt(id_vars=["nodeId"]).rename(
                columns={"variable": "nodeProperty", "value": "propertyValue"}
            )

        return result

    def streamNodeProperty(
        self,
        G: Graph,
        node_properties: str,
        node_labels: Strings = ["*"],
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".streamNodeProperty"

        return self._handle_properties(G, node_properties, node_labels, config)

    def streamRelationshipProperties(
        self,
        G: Graph,
        relationship_properties: List[str],
        relationship_types: Strings = ["*"],
        separate_property_columns: bool = False,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".streamRelationshipProperties"

        result = self._handle_properties(G, relationship_properties, relationship_types, config)

        # new format was requested, but the query was run via Cypher
        if separate_property_columns and "propertyValue" in result.keys():
            result = result.pivot(
                index=["sourceNodeId", "targetNodeId", "relationshipType"],
                columns="relationshipProperty",
                values="propertyValue",
            )
            result = result.reset_index()
            result.columns.name = None
        # old format was requested but the query was run via Arrow
        elif not separate_property_columns and "propertyValue" not in result.keys():
            result = result.melt(id_vars=["sourceNodeId", "targetNodeId", "relationshipType"]).rename(
                columns={"variable": "relationshipProperty", "value": "propertyValue"}
            )

        return result

    def streamRelationshipProperty(
        self,
        G: Graph,
        relationship_properties: str,
        relationship_types: Strings = ["*"],
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".streamRelationshipProperty"

        return self._handle_properties(G, relationship_properties, relationship_types, config)

    def writeNodeProperties(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        **config: Any,
    ) -> "Series[Any]":
        self._namespace += ".writeNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config).squeeze()  # type: ignore

    def writeRelationship(
        self,
        G: Graph,
        relationship_type: str,
        relationship_property: str = "",
        **config: Any,
    ) -> "Series[Any]":
        self._namespace += ".writeRelationship"

        query = f"CALL {self._namespace}($graph_name, $relationship_type, $relationship_property, $config)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
            "relationship_property": relationship_property,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @multimethod
    def removeNodeProperties(self) -> None:
        ...

    @removeNodeProperties.register
    @graph_type_check
    def _(
        self,
        G: Graph,
        node_properties: List[str],
        **config: Any,
    ) -> Series:  # type: ignore
        self._namespace += ".removeNodeProperties"

        query = f"CALL {self._namespace}($graph_name, $properties, $config)"
        params = {
            "graph_name": G.name(),
            "properties": node_properties,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @removeNodeProperties.register
    @compatible_with("removeNodeProperties", max_exclusive=ServerVersion(2, 1, 0))
    @graph_type_check
    def _(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings,
        **config: Any,
    ) -> Series:  # type: ignore
        self._namespace += ".removeNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config).squeeze()  # type: ignore

    @graph_type_check
    def deleteRelationships(self, G: Graph, relationship_type: str) -> "Series[Any]":
        self._namespace += ".deleteRelationships"

        query = f"CALL {self._namespace}($graph_name, $relationship_type)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
