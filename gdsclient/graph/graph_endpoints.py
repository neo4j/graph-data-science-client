from .graph_project_runner import GraphProjectRunner


class GraphEndpoints:
    def __init__(self, query_runner, namespace):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def project(self):
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace)

    def list(self, graph=None):
        self._namespace += ".list"

        if graph:
            query = "CALL gds.graph.list($graph_name)"
            params = {"graph_name": graph.name}
        else:
            query = "CALL gds.graph.list()"
            params = {}

        return self._query_runner.run_query(query, params)
