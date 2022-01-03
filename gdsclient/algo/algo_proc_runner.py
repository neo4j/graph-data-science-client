from typing import Any, Dict

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner


class AlgoProcRunner:
    def __init__(self, query_runner: QueryRunner, proc_name: str):
        self._query_runner = query_runner
        self._proc_name = proc_name

    def _run_procedure(self, G: Graph, config: Dict[str, Any]) -> QueryResult:
        query = f"CALL {self._proc_name}($graph_name, $config)"

        params: Dict[str, Any] = {}
        params["graph_name"] = G.name()
        params["config"] = config

        return self._query_runner.run_query(query, params)

    def __call__(self, G: Graph, **config: Any) -> QueryResult:
        return self._run_procedure(G, config)

    def estimate(self, G: Graph, **config: Any) -> QueryResult:
        self._proc_name += "." + "estimate"
        return self._run_procedure(G, config)
