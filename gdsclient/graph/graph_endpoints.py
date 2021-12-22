from .graph_project_runner import GraphProjectRunner


class GraphEndpoints:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    @property
    def project(self):
        return GraphProjectRunner(self.query_runner)
