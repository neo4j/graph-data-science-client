from typing import Any, Dict, List, Union

from pandas.core.frame import DataFrame
from pandas.core.series import Series

from .graph_object import Graph
from graphdatascience.caller_base import CallerBase
from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion

Strings = Union[str, List[str]]


class GraphEntityOpsBaseRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)

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


class GraphElementPropertyRunner(GraphEntityOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(self, G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame:
        self._namespace += ".stream"
        return self._handle_properties(G, node_properties, node_labels, config)


class GraphNodePropertiesRunner(GraphEntityOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        separate_property_columns: bool = False,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".stream"

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

    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def write(self, G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> Series:
        self._namespace += ".write"
        return self._handle_properties(G, node_properties, node_labels, config).squeeze()  # type: ignore

    @compatible_with("drop", min_inclusive=ServerVersion(2, 2, 0))
    def drop(self, G: Graph, node_properties: List[str], **config: Any) -> Series:
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $properties, $config)"
        params = {
            "graph_name": G.name(),
            "properties": node_properties,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore


class GraphRelationshipPropertiesRunner(GraphEntityOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self,
        G: Graph,
        relationship_properties: List[str],
        relationship_types: Strings = ["*"],
        separate_property_columns: bool = False,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".stream"

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


class GraphRelationshipRunner(GraphEntityOpsBaseRunner):
    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def write(self, G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> Series:
        self._namespace += ".write"
        query = f"CALL {self._namespace}($graph_name, $relationship_type, $relationship_property, $config)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
            "relationship_property": relationship_property,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore


class GraphRelationshipsRunner(GraphEntityOpsBaseRunner):
    @compatible_with("drop", min_inclusive=ServerVersion(2, 2, 0))
    def drop(
        self,
        G: Graph,
        relationship_type: str,
    ) -> Series:
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $relationship_type)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(self, G: Graph, relationship_types: List[str] = ["*"], **config: Any) -> DataFrame:
        self._namespace += ".stream"
        query = f"CALL {self._namespace}($graph_name, $relationship_types, $config)"

        params = {"graph_name": G.name(), "relationship_types": relationship_types, "config": config}

        return self._query_runner.run_query(query, params)


class GraphPropertyRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self,
        G: Graph,
        graph_property: str,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".stream"
        query = f"CALL {self._namespace}($graph_name, $graph_property, $config)"
        params = {"graph_name": G.name(), "graph_property": graph_property, "config": config}

        return self._query_runner.run_query(query, params)

    @compatible_with("drop", min_inclusive=ServerVersion(2, 2, 0))
    def drop(
        self,
        G: Graph,
        graph_property: str,
        **config: Any,
    ) -> Series:
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $graph_property, $config)"
        params = {"graph_name": G.name(), "graph_property": graph_property, "config": config}

        return self._query_runner.run_query(query, params)  # type: ignore
