from typing import Any, Dict, List, Optional, Union

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..query_runner.query_runner import QueryResult, QueryRunner, Row
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
    def create(self) -> GraphProjectRunner:
        self._namespace += ".create"
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
    ) -> Optional[Row]:
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
        if result:
            return result[0]

        return None

    def exists(self, graph_name: str) -> Row:
        self._namespace += ".exists"
        return self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name)", {"graph_name": graph_name}
        )[0]

    def list(self, G: Optional[Graph] = None) -> QueryResult:
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
    ) -> QueryResult:
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
    ) -> QueryResult:
        self._namespace += ".streamNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config)

    def streamNodeProperty(
        self,
        G: Graph,
        node_properties: str,
        node_labels: Strings = ["*"],
        **config: Any,
    ) -> QueryResult:
        self._namespace += ".streamNodeProperty"

        return self._handle_properties(G, node_properties, node_labels, config)

    def streamRelationshipProperties(
        self,
        G: Graph,
        relationship_properties: List[str],
        relationship_types: Strings = ["*"],
        **config: Any,
    ) -> QueryResult:
        self._namespace += ".streamRelationshipProperties"

        return self._handle_properties(
            G, relationship_properties, relationship_types, config
        )

    def streamRelationshipProperty(
        self,
        G: Graph,
        relationship_properties: str,
        relationship_types: Strings = ["*"],
        **config: Any,
    ) -> QueryResult:
        self._namespace += ".streamRelationshipProperty"

        return self._handle_properties(
            G, relationship_properties, relationship_types, config
        )

    def writeNodeProperties(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        **config: Any,
    ) -> Row:
        self._namespace += ".writeNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config)[0]

    def writeRelationship(
        self,
        G: Graph,
        relationship_type: str,
        relationship_property: str = "",
        **config: Any,
    ) -> Row:
        self._namespace += ".writeRelationship"

        query = f"CALL {self._namespace}($graph_name, $relationship_type, $relationship_property, $config)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
            "relationship_property": relationship_property,
            "config": config,
        }

        return self._query_runner.run_query(query, params)[0]

    def removeNodeProperties(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        **config: Any,
    ) -> Row:
        self._namespace += ".removeNodeProperties"

        return self._handle_properties(G, node_properties, node_labels, config)[0]

    def deleteRelationships(self, G: Graph, relationship_type: str) -> Row:
        self._namespace += ".deleteRelationships"

        query = f"CALL {self._namespace}($graph_name, $relationship_type)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
        }

        return self._query_runner.run_query(query, params)[0]

    def generate(
        self, graph_name: str, node_count: int, average_degree: int, **config: Any
    ) -> Graph:
        self._namespace += ".generate"

        query = f"CALL {self._namespace}($graph_name, $node_count, $average_degree, $config)"
        params = {
            "graph_name": graph_name,
            "node_count": node_count,
            "average_degree": average_degree,
            "config": config,
        }

        self._query_runner.run_query(query, params)

        return Graph(graph_name, self._query_runner)
