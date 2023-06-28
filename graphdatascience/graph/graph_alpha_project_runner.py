from typing import Any

from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph.graph_create_result import GraphCreateResult
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion


class GraphAlphaProjectRunner(IllegalAttrChecker):
    @compatible_with("remote", min_inclusive=ServerVersion(2, 4, 0))
    def remote(self, graph_name: str, query: str, remote_database: str, **config: Any) -> GraphCreateResult:
        self._namespace += ".remote"
        procedure_query = f"CALL {self._namespace}($graph_name, $query, $token, $host, $remote_database, $config)"
        params = {"graph_name": graph_name, "query": query, "remote_database": remote_database, "config": config}
        result = self._query_runner.run_query(procedure_query, params).squeeze()
        return GraphCreateResult(Graph(graph_name, self._query_runner, self._server_version), result)
