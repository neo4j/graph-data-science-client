from .algo.algo_endpoints import AlgoEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .utils.direct_util_endpoints import IndirectUtilAlphaEndpoints


class IndirectCallBuilder(AlgoEndpoints, UncallableNamespace):
    def __getattr__(self, attr: str) -> "IndirectCallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return IndirectCallBuilder(self._query_runner, namespace, self._server_version)


class IndirectBetaCallBuilder(AlgoEndpoints, UncallableNamespace):
    def __getattr__(self, attr: str) -> "IndirectBetaCallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return IndirectBetaCallBuilder(self._query_runner, namespace, self._server_version)


class IndirectAlphaCallBuilder(AlgoEndpoints, IndirectUtilAlphaEndpoints, UncallableNamespace):
    def __getattr__(self, attr: str) -> "IndirectAlphaCallBuilder":
        namespace = f"{self._namespace}.{attr}"
        return IndirectAlphaCallBuilder(self._query_runner, namespace, self._server_version)
