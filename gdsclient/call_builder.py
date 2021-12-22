from .algo.algo_endpoints import AlgoEndpoints
from .graph.graph_endpoints import GraphEndpoints


class CallBuilder(AlgoEndpoints, GraphEndpoints):
    def __init__(self, query_runner, namespace):
        super().__init__(query_runner, namespace)
        self.query_runner = query_runner
        self.namespace = namespace

    def __getattr__(self, attr):
        namespace = f"{self.namespace}.{attr}"
        return CallBuilder(self.query_runner, namespace)
