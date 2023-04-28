from typing import Any

from pandas import Series

from graphdatascience.error.client_only_endpoint import client_only_endpoint
from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.graph.graph_object import Graph
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion


class GraphAlphaProjectRunner(IllegalAttrChecker):
    @compatible_with("remote", min_inclusive=ServerVersion(2, 4, 0))
    def remote(self, graph_name: str, query: str, **config: Any) -> "Series[Any]":
        self._namespace += ".remote"
        result = self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name, $query, $config)",
            {"graph_name": graph_name, "query": query, "config": config},
        )

        return result.squeeze()
