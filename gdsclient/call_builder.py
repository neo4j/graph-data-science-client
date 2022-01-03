from typing import Any, NoReturn

from .indirect_endpoints import IndirectEndpoints
from .query_runner.query_runner import QueryRunner


class CallBuilder(IndirectEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        super().__init__(query_runner, namespace)
        self._query_runner = query_runner
        self._namespace = namespace

    def __getattr__(self, attr: str) -> "CallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return CallBuilder(self._query_runner, namespace)

    def __call__(self, *_: Any, **__: Any) -> NoReturn:
        raise SyntaxError(f"There is no {self._namespace} to call")
