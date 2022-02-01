from typing import Any, Dict

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..query_runner.query_runner import QueryRunner, Row
from .graph_object import Graph


class GraphExportRunner(IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def __call__(self, G: Graph, **config: Any) -> Row:
        return self._export_call(G, config)

    def _export_call(self, G: Graph, config: Dict[str, Any]) -> Row:
        query = f"CALL {self._namespace}($graph_name, $config)"
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)[0]

    # TODO: Add an integration test for this call.
    def csv(self, G: Graph, **config: Any) -> Row:
        self._namespace += ".csv"

        return self._export_call(G, config)
