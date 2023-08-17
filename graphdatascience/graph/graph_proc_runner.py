import os
import pathlib
import sys
from typing import Any, ContextManager, Dict, List, Optional, Union

import pandas as pd
from multimethod import multimethod
from pandas import DataFrame, Series, read_pickle

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_entity_ops_runner import (
    GraphElementPropertyRunner,
    GraphNodePropertiesRunner,
    GraphPropertyRunner,
    GraphRelationshipPropertiesRunner,
    GraphRelationshipRunner,
    GraphRelationshipsRunner,
)
from .graph_export_runner import GraphExportRunner
from .graph_object import Graph
from .graph_project_runner import GraphProjectRunner
from .graph_sample_runner import GraphSampleRunner
from .graph_type_check import (
    from_graph_type_check,
    graph_type_check,
    graph_type_check_optional,
)
from .ogb_loader import OGBLLoader, OGBNLoader
from graphdatascience.graph.graph_create_result import GraphCreateResult
from graphdatascience.graph.graph_cypher_runner import GraphCypherRunner

Strings = Union[str, List[str]]


class GraphProcRunner(UncallableNamespace, IllegalAttrChecker):
    @staticmethod
    def _path(package: str, resource: str) -> ContextManager[pathlib.Path]:
        if sys.version_info >= (3, 9):
            import os.path
            from importlib.resources import as_file, files

            return as_file(files(package) / os.path.normpath(resource))
        else:
            from importlib.resources import path

            return path(package, resource)

    @client_only_endpoint("gds.graph")
    @compatible_with("construct", min_inclusive=ServerVersion(2, 1, 0))
    def construct(
        self,
        graph_name: str,
        nodes: Union[DataFrame, List[DataFrame]],
        relationships: Optional[Union[DataFrame, List[DataFrame]]] = None,
        concurrency: int = 4,
        undirected_relationship_types: Optional[List[str]] = None,
    ) -> Graph:
        nodes = nodes if isinstance(nodes, List) else [nodes]

        if isinstance(relationships, DataFrame):
            relationships = [relationships]
        elif relationships is None:
            relationships = []

        # Filter empty dataframes
        nodes = [df for df in nodes if not df.empty]
        relationships = [df for df in relationships if not df.empty]

        errors = []

        exists = self._query_runner.run_query(
            f"CALL gds.graph.exists('{graph_name}') YIELD exists", custom_error=False
        ).squeeze()

        # compare against True as (1) unit tests return None here and (2) numpys True does not work with `is True`.
        if exists == True:  # noqa: E712
            errors.append(
                f"Graph '{graph_name}' already exists. Please drop the existing graph or use a different name."
            )

        for idx, node_df in enumerate(nodes):
            if "nodeId" not in node_df.columns.values:
                errors.append(f"Node dataframe at index {idx} needs to contain a 'nodeId' column.")

        for idx, rel_df in enumerate(relationships):
            for expected_col in ["sourceNodeId", "targetNodeId"]:
                if expected_col not in rel_df.columns.values:
                    errors.append(f"Relationship dataframe at index {idx} needs to contain a '{expected_col}' column.")

        if self._server_version < ServerVersion(2, 3, 0) and undirected_relationship_types:
            errors.append("The parameter 'undirected_relationship_types' is only supported since GDS 2.3.0.")

        if len(errors) > 0:
            raise ValueError(os.linesep.join(errors))

        constructor = self._query_runner.create_graph_constructor(
            graph_name, concurrency, undirected_relationship_types
        )
        constructor.run(nodes, relationships)

        return Graph(graph_name, self._query_runner, self._server_version)

    @client_only_endpoint("gds.graph")
    def load_cora(self, graph_name: str = "cora", undirected: bool = False) -> Graph:
        with self._path("graphdatascience.resources.cora", "cora_nodes_gzip.pkl") as nodes_resource:
            nodes = read_pickle(nodes_resource, compression="gzip")

        with self._path("graphdatascience.resources.cora", "cora_rels_gzip.pkl") as rels_resource:
            rels = read_pickle(rels_resource, compression="gzip")

        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    @client_only_endpoint("gds.graph")
    def load_karate_club(self, graph_name: str = "karate_club", undirected: bool = False) -> Graph:
        nodes = pd.DataFrame({"nodeId": range(1, 35)})
        nodes["labels"] = "Person"

        with self._path("graphdatascience.resources.karate", "karate_club_gzip.pkl") as rels_resource:
            rels = read_pickle(rels_resource, compression="gzip")

        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    @client_only_endpoint("gds.graph")
    def load_imdb(self, graph_name: str = "imdb", undirected: bool = True) -> Graph:
        if self._server_version < ServerVersion(2, 3, 0):
            raise ValueError("The IMDB dataset loading is only supported by GDS 2.3 or later.")

        with self._path("graphdatascience.resources.imdb", "imdb_movies_with_genre_gzip.pkl") as nodes_resource:
            movies_with_genre = read_pickle(nodes_resource, compression="gzip")
        with self._path("graphdatascience.resources.imdb", "imdb_movies_without_genre_gzip.pkl") as nodes_resource:
            movies_without_genre = read_pickle(nodes_resource, compression="gzip")
        with self._path("graphdatascience.resources.imdb", "imdb_actors_gzip.pkl") as nodes_resource:
            actors = read_pickle(nodes_resource, compression="gzip")
        with self._path("graphdatascience.resources.imdb", "imdb_directors_gzip.pkl") as nodes_resource:
            directors = read_pickle(nodes_resource, compression="gzip")

        with self._path("graphdatascience.resources.imdb", "imdb_acted_in_rels_gzip.pkl") as rels_resource:
            acted_in_rels = read_pickle(rels_resource, compression="gzip")
        with self._path("graphdatascience.resources.imdb", "imdb_directed_in_rels_gzip.pkl") as rels_resource:
            directed_in_rels = read_pickle(rels_resource, compression="gzip")

        nodes = [movies_with_genre, movies_without_genre, actors, directors]
        rels = [acted_in_rels, directed_in_rels]

        # Default undirected which matches raw data
        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    @property
    def sample(self) -> GraphSampleRunner:
        self._namespace += ".sample"
        return GraphSampleRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def networkx(self):  # type: ignore
        try:
            from .nx_loader import NXLoader
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "This feature requires NetworkX support. "
                "You can add NetworkX support by running `pip install graphdatascience[networkx]`"
            )

        self._namespace += ".networkx"
        return NXLoader(self._query_runner, self._namespace, self._server_version)

    @property
    def project(self) -> GraphProjectRunner:
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace, self._server_version)

    @property
    @compatible_with("graphProperty", min_inclusive=ServerVersion(2, 5, 0))
    def graphProperty(self) -> GraphPropertyRunner:
        self._namespace += ".graphProperty"
        return GraphPropertyRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def cypher(self) -> GraphCypherRunner:
        self._namespace += ".project"
        return GraphCypherRunner(self._query_runner, self._namespace, self._server_version)

    @compatible_with("generate", min_inclusive=ServerVersion(2, 5, 0))
    def generate(self, graph_name: str, node_count: int, average_degree: int, **config: Any) -> GraphCreateResult:
        self._namespace += ".generate"

        query = f"CALL {self._namespace}($graph_name, $node_count, $average_degree, $config)"
        params = {
            "graph_name": graph_name,
            "node_count": node_count,
            "average_degree": average_degree,
            "config": config,
        }

        result = self._query_runner.run_query(query, params).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)

    @from_graph_type_check
    @compatible_with("filter", min_inclusive=ServerVersion(2, 5, 0))
    def filter(
        self,
        graph_name: str,
        from_G: Graph,
        node_filter: str,
        relationship_filter: str,
        **config: Any,
    ) -> GraphCreateResult:
        self._namespace += ".filter"
        result = self._query_runner.run_query_with_logging(
            f"CALL {self._namespace}($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)",
            {
                "graph_name": graph_name,
                "from_graph_name": from_G.name(),
                "node_filter": node_filter,
                "relationship_filter": relationship_filter,
                "config": config,
            },
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)

    @property
    def export(self) -> GraphExportRunner:
        self._namespace += ".export"
        return GraphExportRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def ogbn(self) -> OGBNLoader:
        self._namespace += ".ogbn"
        return OGBNLoader(self._query_runner, self._namespace, self._server_version)

    @property
    def ogbl(self) -> OGBLLoader:
        self._namespace += ".ogbl"
        return OGBLLoader(self._query_runner, self._namespace, self._server_version)

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
        result = self._query_runner.run_query(
            f"CALL gds.graph.list('{graph_name}') YIELD graphName", custom_error=False
        )
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
