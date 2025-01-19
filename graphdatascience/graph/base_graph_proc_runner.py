from __future__ import annotations

import os
import pathlib
import warnings
from typing import Any, List, Optional, Union

import pandas as pd
from multimethod import multimethod
from neo4j import __version__ as neo4j_driver_version
from pandas import DataFrame, Series, read_parquet

from ..call_parameters import CallParameters
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_create_result import GraphCreateResult
from .graph_entity_ops_runner import (
    GraphLabelRunner,
    GraphNodePropertiesRunner,
    GraphNodePropertyRunner,
    GraphPropertyRunner,
    GraphRelationshipPropertiesRunner,
    GraphRelationshipPropertyRunner,
    GraphRelationshipRunner,
    GraphRelationshipsRunner,
)
from .graph_export_runner import GraphExportRunner
from .graph_object import Graph
from .graph_sample_runner import GraphSampleRunner
from .graph_type_check import (
    from_graph_type_check,
    graph_type_check,
)
from .ogb_loader import OGBLLoader, OGBNLoader

Strings = Union[str, List[str]]

is_neo4j_4_driver = ServerVersion.from_string(neo4j_driver_version) < ServerVersion(5, 0, 0)


class BaseGraphProcRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: Any, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)
        # Pandas 2.2.0 deprecated an API used by ArrowTable.to_pandas() (< pyarrow 14.0)
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message=r"Passing a BlockManager to DataFrame is deprecated",
        )

    @staticmethod
    def _path(package: str, resource: str) -> pathlib.Path:
        from importlib.resources import files

        # files() returns a Traversable, but usages require a Path object
        return pathlib.Path(str(files(package) / resource))

    @client_only_endpoint("gds.graph")
    @compatible_with("construct", min_inclusive=ServerVersion(2, 1, 0))
    def construct(
        self,
        graph_name: str,
        nodes: Union[DataFrame, list[DataFrame]],
        relationships: Optional[Union[DataFrame, list[DataFrame]]] = None,
        concurrency: int = 4,
        undirected_relationship_types: Optional[list[str]] = None,
    ) -> Graph:
        nodes = nodes if isinstance(nodes, list) else [nodes]

        if isinstance(relationships, DataFrame):
            relationships = [relationships]
        elif relationships is None:
            relationships = []

        # Filter empty dataframes
        nodes = [df for df in nodes if not df.empty]
        relationships = [df for df in relationships if not df.empty]

        errors = []

        exists = self._query_runner.call_procedure(
            endpoint="gds.graph.exists",
            params=CallParameters(graph_name=graph_name),
            yields=["exists"],
            custom_error=False,
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

        return Graph(graph_name, self._query_runner)

    @client_only_endpoint("gds.graph")
    def load_cora(self, graph_name: str = "cora", undirected: bool = False) -> Graph:
        file = self._path("graphdatascience.resources.cora", "cora_nodes.parquet.gzip")
        nodes = read_parquet(file)

        if is_neo4j_4_driver:
            # features is read as an ndarray which was not supported in neo4j 4
            nodes["features"] = nodes["features"].apply(lambda x: x.tolist())

        rels = read_parquet(self._path("graphdatascience.resources.cora", "cora_rels.parquet.gzip"))

        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    @client_only_endpoint("gds.graph")
    def load_karate_club(self, graph_name: str = "karate_club", undirected: bool = False) -> Graph:
        nodes = pd.DataFrame({"nodeId": range(1, 35)})
        nodes["labels"] = "Person"

        rels = read_parquet(self._path("graphdatascience.resources.karate", "karate_club.parquet.gzip"))

        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(graph_name, nodes, rels, undirected_relationship_types=undirected_relationship_types)

    @client_only_endpoint("gds.graph")
    def load_imdb(self, graph_name: str = "imdb", undirected: bool = True) -> Graph:
        if self._server_version < ServerVersion(2, 3, 0):
            raise ValueError("The IMDB dataset loading is only supported by GDS 2.3 or later.")

        package = "graphdatascience.resources.imdb"
        nodes = ["movies_with_genre", "movies_without_genre", "actors", "directors"]
        rels = ["acted_in", "directed_in"]

        node_dfs = []
        for n in nodes:
            resource = self._path(package, f"imdb_{n}.parquet.gzip")
            df = read_parquet(resource)
            if is_neo4j_4_driver:
                # features is read as an ndarray which was not supported in neo4j 4
                df["plot_keywords"] = df["plot_keywords"].apply(lambda x: x.tolist())
            node_dfs.append(df)

        rel_dfs = []
        for r in rels:
            resource = self._path(package, f"imdb_{r}.parquet.gzip")
            rel_dfs.append(read_parquet(resource))

        # Default undirected which matches raw data
        undirected_relationship_types = ["*"] if undirected else []

        return self.construct(
            graph_name, node_dfs, rel_dfs, undirected_relationship_types=undirected_relationship_types
        )

    @client_only_endpoint("gds.graph")
    def load_lastfm(self, graph_name: str = "lastfm", undirected: bool = True) -> Any:
        if self._server_version < ServerVersion(2, 3, 0):
            raise ValueError("The LastFM2K dataset loading is only supported by GDS 2.3 or later.")

        nodes = ["user_nodes", "artist_nodes"]
        rels = ["user_friend_df_directed", "user_listen_artist_rels", "user_tag_artist_rels"]

        package = "graphdatascience.resources.lastfm"

        node_dfs = []
        for n in nodes:
            resource = self._path(package, f"{n}.parquet.gzip")
            node_dfs.append(read_parquet(resource))

        rel_dfs = []
        for r in rels:
            resource = self._path(package, f"{r}.parquet.gzip")
            rel_dfs.append(read_parquet(resource))

        # Default undirected for usage in GDS ML pipelines
        if undirected:
            undirected_relationship_types = ["LISTEN_TO", "TAGGED", "IS_FRIEND"]
        else:
            undirected_relationship_types = []

        return self.construct(
            graph_name, node_dfs, rel_dfs, undirected_relationship_types=undirected_relationship_types
        )

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
    @compatible_with("graphProperty", min_inclusive=ServerVersion(2, 5, 0))
    def graphProperty(self) -> GraphPropertyRunner:
        self._namespace += ".graphProperty"
        return GraphPropertyRunner(self._query_runner, self._namespace, self._server_version)

    @property
    @compatible_with("nodeLabel", min_inclusive=ServerVersion(2, 5, 0))
    def nodeLabel(self) -> GraphLabelRunner:
        self._namespace += ".nodeLabel"
        return GraphLabelRunner(self._query_runner, self._namespace, self._server_version)

    @compatible_with("generate", min_inclusive=ServerVersion(2, 5, 0))
    def generate(self, graph_name: str, node_count: int, average_degree: int, **config: Any) -> GraphCreateResult:
        self._namespace += ".generate"
        params = CallParameters(
            graph_name=graph_name, node_count=node_count, average_degree=average_degree, config=config
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)

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
        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=from_G.name(),
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            logging=True,
            params=params,
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)

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

    def drop(
        self,
        graph: Union[Graph, str],
        failIfMissing: bool = False,
        dbName: str = "",
        username: Optional[str] = None,
    ) -> Optional[Series[Any]]:
        self._namespace += ".drop"

        if isinstance(graph, Graph):
            graph = graph.name()

        params = CallParameters(
            graph_name=graph,
            fail_if_missing=failIfMissing,
            db_name=dbName,
        )
        if username:
            params["username"] = username

        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
        )
        if not result.empty:
            return result.squeeze()  # type: ignore

        return None

    def exists(self, graph_name: str) -> Series[Any]:
        self._namespace += ".exists"
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=CallParameters(graph_name=graph_name),
        )

        return result.squeeze()  # type: ignore

    def list(self, G: Optional[Union[Graph, str]] = None) -> DataFrame:
        self._namespace += ".list"

        if isinstance(G, Graph):
            graph_name = G.name()
        elif isinstance(G, str):
            graph_name = G

        params = CallParameters()
        if G:
            params["graph_name"] = graph_name

        return self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
        )

    @client_only_endpoint("gds.graph")
    def get(self, graph_name: str) -> Graph:
        result = self._query_runner.call_procedure(
            endpoint="gds.graph.list",
            params=CallParameters(graph_name=graph_name),
            yields=["graphName"],
            custom_error=False,
        )
        if len(result["graphName"]) == 0:
            raise ValueError(
                f"No projected graph named '{graph_name}' exists in current database '{self._query_runner.database()}'"
            )

        return Graph(graph_name, self._query_runner)

    @graph_type_check
    def _handle_properties(
        self,
        G: Graph,
        properties: Strings,
        entities: Strings,
        config: dict[str, Any],
    ) -> DataFrame:
        params = CallParameters(
            graph_name=G.name(),
            properties=properties,
            entities=entities,
            config=config,
        )

        return self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
        )

    @property
    def nodeProperty(self) -> GraphNodePropertyRunner:
        self._namespace += ".nodeProperty"
        return GraphNodePropertyRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def nodeProperties(self) -> GraphNodePropertiesRunner:
        self._namespace += ".nodeProperties"
        return GraphNodePropertiesRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def relationshipProperty(self) -> GraphRelationshipPropertyRunner:
        self._namespace += ".relationshipProperty"
        return GraphRelationshipPropertyRunner(self._query_runner, self._namespace, self._server_version)

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
        params = CallParameters(
            graph_name=G.name(),
            relationship_type=relationship_type,
            relationship_property=relationship_property,
            config=config,
        )

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace,
            params=params,
        ).squeeze()

    @multimethod
    def removeNodeProperties(self) -> None: ...

    @removeNodeProperties.register
    @graph_type_check
    def _(
        self,
        G: Graph,
        node_properties: List[str],
        **config: Any,
    ) -> Series:  # type: ignore
        self._namespace += ".removeNodeProperties"
        params = CallParameters(
            graph_name=G.name(),
            properties=node_properties,
            config=config,
        )

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace,
            params=params,
        ).squeeze()

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

        params = CallParameters(
            graph_name=G.name(),
            relationship_type=relationship_type,
        )

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace,
            params=params,
        ).squeeze()
