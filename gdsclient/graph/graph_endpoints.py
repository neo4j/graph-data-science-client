from .graph_project_runner import GraphProjectRunner


class GraphEndpoints:
    def __init__(self, query_runner):
        self._query_runner = query_runner

    @property
    def project(self):
        return GraphProjectRunner(self._query_runner)
