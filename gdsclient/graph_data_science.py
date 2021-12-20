from .graph import GraphProcBuilder
from .algo_proc_builder import AlgoProcBuilder


class GraphDataScience:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    @property
    def graph(self):
        return GraphProcBuilder(self.query_runner)

    def __getattr__(self, attr):
        return getattr(AlgoProcBuilder(self.query_runner), attr)
