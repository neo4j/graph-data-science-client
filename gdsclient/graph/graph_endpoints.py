from .graph_project_runner import GraphProjectRunner


class GraphEndpoints:
    def __init__(self, query_runner, namespace):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def project(self):
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace)
