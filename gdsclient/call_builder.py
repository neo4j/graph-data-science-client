from .algo.algo_endpoints import AlgoEndpoints
from .graph.graph_endpoints import GraphEndpoints


class CallBuilder(AlgoEndpoints, GraphEndpoints):
    def __init__(self, query_runner, namespace):
        super().__init__(query_runner, namespace)
        self._query_runner = query_runner
        self._namespace = namespace

    def __getattr__(self, attr):
        namespace = f"{self._namespace}.{attr}"
        return CallBuilder(self._query_runner, namespace)

    def __call__(self, *args, **kwargs):
        raise SyntaxError(f"There is no {self._namespace} to call")
