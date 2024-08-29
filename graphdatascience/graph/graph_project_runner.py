from __future__ import annotations

from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from .graph_create_result import GraphCreateResult
from .graph_object import Graph
from .graph_type_check import from_graph_type_check


class GraphProjectRunner(IllegalAttrChecker):
    def __call__(self, graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> GraphCreateResult:
        params = CallParameters(
            graph_name=graph_name,
            node_spec=node_spec,
            relationship_spec=relationship_spec,
            config=config,
        )
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
            logging=True,
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)

    def estimate(self, node_projection: Any, relationship_projection: Any, **config: Any) -> "Series[Any]":
        self._namespace += ".estimate"
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=CallParameters(
                node_spec=node_projection,
                relationship_spec=relationship_projection,
                config=config,
            ),
        )

        return result.squeeze()  # type: ignore

    @property
    def cypher(self) -> GraphProjectRunner:
        return GraphProjectRunner(self._query_runner, self._namespace + ".cypher", self._server_version)


class GraphProjectBetaRunner(IllegalAttrChecker):
    @from_graph_type_check
    def subgraph(
        self,
        graph_name: str,
        from_G: Graph,
        node_filter: str,
        relationship_filter: str,
        **config: Any,
    ) -> GraphCreateResult:
        self._namespace += ".subgraph"
        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=from_G.name(),
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            config=config,
        )
        result = self._query_runner.call_procedure(
            endpoint=self._namespace,
            params=params,
            logging=True,
        ).squeeze()

        return GraphCreateResult(Graph(graph_name, self._query_runner), result)
