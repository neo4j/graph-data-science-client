import os
from typing import Any, Dict, List, Optional, Tuple, Union

from multimethod import multimethod
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_export_runner import GraphExportRunner
from .graph_object import Graph
from .graph_project_runner import GraphProjectRunner
from .graph_sample_runner import GraphSampleRunner
from graphdatascience.graph.graph_entity_ops_runner import (
    GraphElementPropertyRunner,
    GraphNodePropertiesRunner,
    GraphPropertyRunner,
    GraphRelationshipPropertiesRunner,
    GraphRelationshipRunner,
    GraphRelationshipsRunner,
)

Strings = Union[str, List[str]]


class GraphProcRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    @property
    def project(self) -> GraphProjectRunner:
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def export(self) -> GraphExportRunner:
        self._namespace += ".export"
        return GraphExportRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def sample(self) -> GraphSampleRunner:
        self._namespace += ".sample"
        return GraphSampleRunner(self._query_runner, self._namespace, self._server_version)

    def drop(
        self,
        G: Graph,
        failIfMissing: bool = False,
        dbName: str = "",
        username: Optional[str] = None,
    ) -> Optional[Series]:
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

    def exists(self, graph_name: str) -> Series:
        self._namespace += ".exists"
        result = self._query_runner.run_query(f"CALL {self._namespace}($graph_name)", {"graph_name": graph_name})

        return result.squeeze()  # type: ignore

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
        if not self.exists(graph_name)["exists"]:
            raise ValueError(
                f"No projected graph named '{graph_name}' exists in current database '{self._query_runner.database()}'"
            )

        return Graph(graph_name, self._query_runner, self._server_version)

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

    @property
    def graphProperty(self) -> GraphPropertyRunner:
        self._namespace += ".graphProperty"
        return GraphPropertyRunner(self._query_runner, self._namespace, self._server_version)

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
    ) -> Series:
        self._namespace += ".writeNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config).squeeze()  # type: ignore

    def writeRelationship(
        self,
        G: Graph,
        relationship_type: str,
        relationship_property: str = "",
        **config: Any,
    ) -> Series:
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
    def _(
        self,
        G: Graph,
        node_properties: List[str],
        **config: Any,
    ) -> Series:
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
    def _(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings,
        **config: Any,
    ) -> Series:
        self._namespace += ".removeNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config).squeeze()  # type: ignore

    def deleteRelationships(self, G: Graph, relationship_type: str) -> Series:
        self._namespace += ".deleteRelationships"

        query = f"CALL {self._namespace}($graph_name, $relationship_type)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def generate(self, graph_name: str, node_count: int, average_degree: int, **config: Any) -> Tuple[Graph, Series]:
        self._namespace += ".generate"

        query = f"CALL {self._namespace}($graph_name, $node_count, $average_degree, $config)"
        params = {
            "graph_name": graph_name,
            "node_count": node_count,
            "average_degree": average_degree,
            "config": config,
        }

        result = self._query_runner.run_query(query, params).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result

    @client_only_endpoint("gds.alpha.graph")
    @compatible_with("construct", min_inclusive=ServerVersion(2, 1, 0))
    def construct(
        self,
        graph_name: str,
        nodes: Union[DataFrame, List[DataFrame]],
        relationships: Union[DataFrame, List[DataFrame]],
        concurrency: int = 4,
    ) -> Graph:
        nodes = nodes if isinstance(nodes, List) else [nodes]
        relationships = relationships if isinstance(relationships, List) else [relationships]

        errors = []
        for idx, node_df in enumerate(nodes):
            if "nodeId" not in node_df.columns.values:
                errors.append(f"Node dataframe at index {idx} needs to contain a 'nodeId' column.")

        for idx, rel_df in enumerate(relationships):
            for expected_col in ["sourceNodeId", "targetNodeId"]:
                if expected_col not in rel_df.columns.values:
                    errors.append(f"Relationship dataframe at index {idx} needs to contain a '{expected_col}' column.")

        if len(errors) > 0:
            raise ValueError(os.linesep.join(errors))

        constructor = self._query_runner.create_graph_constructor(graph_name, concurrency)
        constructor.run(nodes, relationships)

        return Graph(graph_name, self._query_runner, self._server_version)
