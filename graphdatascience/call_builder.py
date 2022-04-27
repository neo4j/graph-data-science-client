from .error.uncallable_namespace import UncallableNamespace
from .indirect_endpoints import IndirectEndpoints


class CallBuilder(IndirectEndpoints, UncallableNamespace):
    def __getattr__(self, attr: str) -> "CallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return CallBuilder(self._query_runner, namespace, self._server_version)
