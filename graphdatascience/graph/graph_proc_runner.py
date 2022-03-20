from typing import Any, Dict, List, Optional, Tuple, Union

from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..query_runner.query_runner import QueryRunner
from .graph_export_runner import GraphExportRunner
from .graph_object import Graph
from .graph_project_runner import GraphProjectRunner

Strings = Union[str, List[str]]


class GraphProcRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def project(self) -> GraphProjectRunner:
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace)

    @property
    def export(self) -> GraphExportRunner:
        self._namespace += ".export"
        return GraphExportRunner(self._query_runner, self._namespace)

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
            raise ValueError(f"No projected graph named '{graph_name}' exists")

        return Graph(graph_name, self._query_runner)

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

    def streamNodeProperties(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".streamNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config)

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
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".streamRelationshipProperties"

        return self._handle_properties(G, relationship_properties, relationship_types, config)

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

    def removeNodeProperties(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
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

        return Graph(graph_name, self._query_runner), result
