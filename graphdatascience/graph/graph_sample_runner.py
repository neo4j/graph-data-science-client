from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_create_result import GraphCreateResult
from .graph_object import Graph
from .graph_type_check import from_graph_type_check


class GraphAlphaSampleRunner(IllegalAttrChecker):
    @compatible_with("construct", min_inclusive=ServerVersion(2, 2, 0))
    @from_graph_type_check
    def rwr(self, graph_name: str, from_G: Graph, **config: Any) -> GraphCreateResult:
        runner = RWRRunner(self._query_runner, self._namespace + ".rwr", self._server_version)
        return runner(graph_name, from_G, **config)


class GraphSampleRunner(IllegalAttrChecker):
    @property
    def rwr(self) -> "RWRRunner":
        return RWRRunner(self._query_runner, self._namespace + ".rwr", self._server_version)

    @property
    def cnarw(self) -> "CNARWRunner":
        return CNARWRunner(self._query_runner, self._namespace + ".cnarw", self._server_version)


class RWRRunner(IllegalAttrChecker):
    @compatible_with("construct", min_inclusive=ServerVersion(2, 2, 0))
    @from_graph_type_check
    def __call__(self, graph_name: str, from_G: Graph, **config: Any) -> GraphCreateResult:
        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=from_G.name(),
            config=config,
        )

        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params, logging=True).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)


class CNARWRunner(IllegalAttrChecker):
    @compatible_with("construct", min_inclusive=ServerVersion(2, 4, 0))
    @from_graph_type_check
    def __call__(self, graph_name: str, from_G: Graph, **config: Any) -> GraphCreateResult:
        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=from_G.name(),
            config=config,
        )
        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params, logging=True).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)

    def estimate(self, from_G: Graph, **config: Any) -> "Series[Any]":
        self._namespace += ".estimate"
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=CallParameters(
                from_graph_name=from_G.name(),
                config=config,
            ),
        )

        return result.squeeze()  # type: ignore
