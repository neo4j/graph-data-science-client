from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .graph_object import Graph
from .graph_type_check import graph_type_check


class GraphExportCsvRunner(IllegalAttrChecker):
    # TODO: Add an integration test for this call.
    def __call__(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._export_call(G, config)

    @graph_type_check
    def _export_call(self, G: Graph, config: dict[str, Any]) -> "Series[Any]":
        params = CallParameters(graph_name=G.name(), config=config)
        return self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()  # type: ignore

    @graph_type_check
    def estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        self._namespace += ".estimate"

        return self._export_call(G, config)


class GraphExportCsvEndpoints(UncallableNamespace, IllegalAttrChecker):
    @property
    def csv(self) -> GraphExportCsvRunner:
        self._namespace += ".csv"

        return GraphExportCsvRunner(self._query_runner, self._namespace, self._server_version)


class GraphExportRunner(IllegalAttrChecker):
    def __call__(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._export_call(G, config)

    @graph_type_check
    def _export_call(self, G: Graph, config: dict[str, Any]) -> "Series[Any]":
        params = CallParameters(graph_name=G.name(), config=config)
        return self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()  # type: ignore

    @property
    def csv(self) -> GraphExportCsvRunner:
        self._namespace += ".csv"

        return GraphExportCsvRunner(self._query_runner, self._namespace, self._server_version)
