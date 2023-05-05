from typing import Any

from pandas import Series

from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion


class GraphAlphaProjectRunner(IllegalAttrChecker):
    @compatible_with("remote", min_inclusive=ServerVersion(2, 4, 0))
    def remote(self, graph_name: str, query: str, remote_database: str, **config: Any) -> "Series[Any]":
        self._namespace += ".remote"
        query = f"CALL {self._namespace}($graph_name, $query, $token, $host, $remote_database, $config)"
        params = {"graph_name": graph_name, "query": query, "remote_database": remote_database, "config": config}
        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
