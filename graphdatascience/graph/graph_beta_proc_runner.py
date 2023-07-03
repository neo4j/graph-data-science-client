from typing import Any, List, Union

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .graph_export_runner import GraphExportCsvEndpoints
from .graph_object import Graph
from .graph_project_runner import GraphProjectBetaRunner
from graphdatascience.graph.graph_create_result import GraphCreateResult
from graphdatascience.graph.graph_entity_ops_runner import GraphRelationshipsBetaRunner

Strings = Union[str, List[str]]


class GraphBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def project(self) -> GraphProjectBetaRunner:
        self._namespace += ".project"
        return GraphProjectBetaRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def export(self) -> GraphExportCsvEndpoints:
        self._namespace += ".export"
        return GraphExportCsvEndpoints(self._query_runner, self._namespace, self._server_version)

    @property
    def relationships(self) -> GraphRelationshipsBetaRunner:
        self._namespace += ".relationships"
        return GraphRelationshipsBetaRunner(self._query_runner, self._namespace, self._server_version)

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
