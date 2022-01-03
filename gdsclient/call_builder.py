from typing import Any, NoReturn

from .algo.algo_endpoints import AlgoEndpoints
from .graph.graph_endpoints import GraphEndpoints
from .query_runner.query_runner import QueryRunner


class CallBuilder(AlgoEndpoints, GraphEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        super().__init__(query_runner, namespace)
        self._query_runner = query_runner
        self._namespace = namespace

    def __getattr__(self, attr: str) -> "CallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return CallBuilder(self._query_runner, namespace)

    def __call__(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise SyntaxError(f"There is no {self._namespace} to call")
