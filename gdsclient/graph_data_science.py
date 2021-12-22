from .algo_proc_builder import AlgoEndpoints
from .graph import GraphEndpoints


class GraphDataScience:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    def __getattr__(self, attr):
        return getattr(CallBuilder(self.query_runner, "gds"), attr)


class CallBuilder(AlgoEndpoints, GraphEndpoints):
    def __init__(self, query_runner, namespace):
        super().__init__(query_runner, namespace)
        self.query_runner = query_runner
        self.namespace = namespace

    def __getattr__(self, attr):
        namespace = f"{self.namespace}.{attr}"
        return CallBuilder(self.query_runner, namespace)
