from typing import Any, Union

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .graph_create_result import GraphCreateResult
from .graph_entity_ops_runner import GraphRelationshipsBetaRunner
from .graph_export_runner import GraphExportCsvEndpoints
from .graph_object import Graph
from .graph_project_runner import GraphProjectBetaRunner

Strings = Union[str, list[str]]


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

        params = CallParameters(
            graph_name=graph_name,
            node_count=node_count,
            average_degree=average_degree,
            config=config,
        )

        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)
