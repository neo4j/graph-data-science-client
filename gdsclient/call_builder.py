from typing import Any, NoReturn

from .indirect_endpoints import IndirectEndpoints


class CallBuilder(IndirectEndpoints):
    def __getattr__(self, attr: str) -> "CallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return CallBuilder(self._query_runner, namespace)

    def __call__(self, *_: Any, **__: Any) -> NoReturn:
        raise SyntaxError(f"There is no {self._namespace} to call")
