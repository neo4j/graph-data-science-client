
from calendar import c
from typing import Any, Callable, Dict, Union, List
from pandas.core.frame import DataFrame

from graphdatascience.caller_base import CallerBase
from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion
from .graph_object import Graph

Strings = Union[str, List[str]]


class GraphOpsBaseRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def __init__(
        self, 
        query_runner: QueryRunner, 
        namespace: str, 
        server_version: ServerVersion,
        handle_properties: Callable[[Graph, Strings, Strings, Dict[str, Any]], DataFrame]
    ):
        super().__init__(query_runner, namespace, server_version)
        self._handle_properties = handle_properties

class GraphPropertyRunner(GraphOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self, 
        G: Graph, 
        property: str, 
        element_identifiers: List[str], 
        **config: Any
    ) -> DataFrame:
        self._namespace += ".stream"
        return self._handle_properties(G, property, element_identifiers, config)

class GraphPropertiesRunner(GraphOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self, 
        G: Graph, 
        properties: List[str], 
        element_identifiers: List[str], 
        **config: Any
    ) -> DataFrame:
        self._namespace += ".stream"
        return self._handle_properties(G, properties, element_identifiers, config)

class GraphNodePropertiesRunner(GraphPropertiesRunner):
    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def write(
        self, 
        G: Graph, 
        node_properties: List[str], 
        node_labels: List[str], 
        **config: Any
    ) -> DataFrame:
        self._namespace += ".write"
        return self._handle_properties(G, node_properties, node_labels, config)

    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def drop(
        self, 
        G: Graph, 
        node_properties: List[str], 
        **config: Any
    ) -> DataFrame:
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $properties, $config)"
        params = {
            "graph_name": G.name(),
            "properties": node_properties,
            "config": config,
        }

        return self._query_runner.run_query(query, params)

class GraphRelationshipRunner(GraphOpsBaseRunner):
    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def write(
        self, 
        G: Graph, 
        relationship_type: str, 
        relationship_property: str, 
        **config: Any
    ) -> DataFrame:
        self._namespace += ".write"
        return self._handle_properties(G, relationship_property, relationship_type, config)

class GraphRelationshipsRunner(GraphOpsBaseRunner):
    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def drop(
        self, 
        G: Graph, 
        relationship_type: str, 
    ) -> DataFrame:
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $relationship_type)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
        }

        return self._query_runner.run_query(query, params)