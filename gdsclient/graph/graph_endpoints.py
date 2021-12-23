from ..validation import validation
from .graph_project_runner import GraphProjectRunner


class GraphEndpoints:
    def __init__(self, query_runner, namespace):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def project(self):
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace)

    @validation.assert_graph(args_pos=1)
    def drop(self, graph, failIfMissing=False, dbName="", username=None):
        self._namespace += ".drop"

        params = {
            "graph_name": graph.name(),
            "fail_if_missing": failIfMissing,
            "db_name": dbName,
        }
        if username:
            query = f"CALL {self._namespace}($graph_name, $fail_if_missing, $db_name, $username)"
            params["username"] = username
        else:
            query = f"CALL {self._namespace}($graph_name, $fail_if_missing, $db_name)"

        return self._query_runner.run_query(query, params)

    @validation.assert_graph(args_pos=1)
    def exists(self, graph):
        self._namespace += ".exists"
        return self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name)", {"graph_name": graph.name()}
        )

    @validation.assert_graph(key="graph")
    def list(self, graph=None):
        self._namespace += ".list"

        if graph:
            query = f"CALL {self._namespace}($graph_name)"
            params = {"graph_name": graph.name()}
        else:
            query = "CALL gds.graph.list()"
            params = {}

        return self._query_runner.run_query(query, params)

    @validation.assert_graph(args_pos=1)
    def export(self, graph, **config):
        self._namespace += ".export"

        query = f"CALL {self._namespace}($graph_name, $config)"

        params = {"graph_name": graph.name(), "config": config}

        return self._query_runner.run_query(query, params)
