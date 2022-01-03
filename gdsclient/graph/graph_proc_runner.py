from typing import Any, Optional

from gdsclient.query_runner.query_runner import QueryResult, QueryRunner

from .graph_object import Graph
from .graph_project_runner import GraphProjectRunner


class GraphProcRunner:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def project(self) -> GraphProjectRunner:
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace)

    def drop(
        self,
        G: Graph,
        failIfMissing: bool = False,
        dbName: str = "",
        username: Optional[str] = None,
    ) -> QueryResult:
        self._namespace += ".drop"

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

        return result

    def exists(self, graph_name: str) -> QueryResult:
        self._namespace += ".exists"
        return self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name)", {"graph_name": graph_name}
        )

    def list(self, G: Optional[Graph] = None) -> QueryResult:
        self._namespace += ".list"

        if G:
            query = f"CALL {self._namespace}($graph_name)"
            params = {"graph_name": G.name()}
        else:
            query = "CALL gds.graph.list()"
            params = {}

        return self._query_runner.run_query(query, params)

    def export(self, G: Graph, **config: Any) -> QueryResult:
        self._namespace += ".export"

        query = f"CALL {self._namespace}($graph_name, $config)"

        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)

    def get(self, graph_name: str) -> Graph:
        if self._namespace != "gds.graph":
            raise SyntaxError(f"There is no {self._namespace + '.get'} to call")

        if not self.exists(graph_name)[0]["exists"]:
            raise ValueError(f"No projected graph named '{graph_name}' exists")

        return Graph(graph_name, self._query_runner)
