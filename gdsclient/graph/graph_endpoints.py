from ..validation import validation
from .graph_object import Graph
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
    def drop(self, G, failIfMissing=False, dbName="", username=None):
        if self._namespace != "gds.graph":
            raise SyntaxError(f"There is no {self._namespace + '.drop'} to call")

        # Make sure graph is marked as dropped if not existing.
        if not self.exists(G.name())[0]["exists"]:
            G._dropped = True

        self._namespace = "gds.graph.drop"

        params = {
            "graph_name": G.name(),
            "fail_if_missing": failIfMissing,
            "db_name": dbName,
        }
        if username:
            query = f"CALL {self._namespace}($graph_name, $fail_if_missing, $db_name, $username)"
            params["username"] = username
        else:
            query = f"CALL {self._namespace}($graph_name, $fail_if_missing, $db_name)"

        result = self._query_runner.run_query(query, params)
        G._dropped = True

        return result

    def exists(self, graph_name):
        self._namespace += ".exists"
        return self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name)", {"graph_name": graph_name}
        )

    @validation.assert_graph(key="G")
    def list(self, G=None):
        self._namespace += ".list"

        if G:
            query = f"CALL {self._namespace}($graph_name)"
            params = {"graph_name": G.name()}
        else:
            query = "CALL gds.graph.list()"
            params = {}

        return self._query_runner.run_query(query, params)

    @validation.assert_graph(args_pos=1)
    def export(self, G, **config):
        self._namespace += ".export"

        query = f"CALL {self._namespace}($graph_name, $config)"

        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)

    def get(self, graph_name):
        if self._namespace != "gds.graph":
            raise SyntaxError(f"There is no {self._namespace + '.get'} to call")

        if not self.exists(graph_name)[0]["exists"]:
            raise ValueError(f"No projected graph named '{graph_name}' exists")

        return Graph(graph_name, self._query_runner)
