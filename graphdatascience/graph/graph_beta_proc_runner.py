from typing import Any, List, Tuple, Union

from pandas import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .graph_export_runner import GraphExportCsvEndpoints
from .graph_object import Graph
from .graph_project_runner import GraphProjectBetaRunner
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

    def generate(
        self, graph_name: str, node_count: int, average_degree: int, **config: Any
    ) -> Tuple[Graph, "Series[Any]"]:
        """
        Generate a random graph.

        Args:
            graph_name: Name of the graph to generate.
            node_count: Number of nodes in the graph.
            average_degree: Average degree of the graph.
            **config: Additional configuration parameters.

        Returns:
            Generated graph and size of the generated graph.

        """
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
