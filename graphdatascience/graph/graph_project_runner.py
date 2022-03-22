from typing import Any, Tuple

from pandas.core.series import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..query_runner.query_runner import QueryRunner
from .graph_object import Graph


class GraphProjectRunner(IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def __call__(self, graph_name: str, node_spec: Any, relationship_spec: Any, **config: Any) -> Tuple[Graph, Series]:
        result = self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name, $node_spec, $relationship_spec, $config)",
            {
                "graph_name": graph_name,
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
                "config": config,
            },
        ).squeeze()

        return Graph(graph_name, self._query_runner), result

    def estimate(self, node_spec: Any, relationship_spec: Any, **config: Any) -> Series:
        self._namespace += ".estimate"
        result = self._query_runner.run_query(
            f"CALL {self._namespace}($node_spec, $relationship_spec, $config)",
            {
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
                "config": config,
            },
        )

        return result.squeeze()  # type: ignore

    @property
    def cypher(self) -> "GraphProjectRunner":
        return GraphProjectRunner(self._query_runner, self._namespace + ".cypher")

    def subgraph(
        self,
        graph_name: str,
        from_G: Graph,
        node_filter: str,
        relationship_filter: str,
        **config: Any,
    ) -> Tuple[Graph, Series]:
        self._namespace += ".subgraph"
        result = self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)",
            {
                "graph_name": graph_name,
                "from_graph_name": from_G.name(),
                "node_filter": node_filter,
                "relationship_filter": relationship_filter,
                "config": config,
            },
        ).squeeze()

        return Graph(graph_name, self._query_runner), result
