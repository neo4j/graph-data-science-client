from .graph_proc import GraphProc
from .algo_proc_builder import AlgoProcBuilder


class GraphDataScience:
    def __init__(self, query_runner):
        self.query_runner = query_runner
        self.graph_proc = GraphProc(query_runner)

    @property
    def graph(self):
        return self.graph_proc

    def __getattr__(self, attr):
        return getattr(AlgoProcBuilder(self.query_runner), attr)
