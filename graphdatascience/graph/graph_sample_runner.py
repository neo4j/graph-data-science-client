from typing import Any, Tuple

from pandas import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_object import Graph
from .graph_type_check import from_graph_type_check


class GraphSampleRunner(IllegalAttrChecker):
    @compatible_with("construct", min_inclusive=ServerVersion(2, 2, 0))
    @from_graph_type_check
    def rwr(self, graph_name: str, from_G: Graph, **config: Any) -> Tuple[Graph, "Series[Any]"]:
        """
        Creates a new graph by sampling a given graph using the Random Walks with Restarts algorithm.

        Args:
            graph_name: the name to give the new graph in the catalog.
            from_G: the graph to sample from.
            **config: the configuration for the algorithm.

        Returns:

        """
        self._namespace += ".rwr"

        query = f"CALL {self._namespace}($graph_name, $from_graph_name, $config)"
        params = {
            "graph_name": graph_name,
            "from_graph_name": from_G.name(),
            "config": config,
        }

        result = self._query_runner.run_query_with_logging(query, params).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result

    @compatible_with("construct", min_inclusive=ServerVersion(2, 4, 0))
    @from_graph_type_check
    def cnarw(self, graph_name: str, from_G: Graph, **config: Any) -> Tuple[Graph, "Series[Any]"]:
        self._namespace += ".cnarw"

        query = f"CALL {self._namespace}($graph_name, $from_graph_name, $config)"
        params = {
            "graph_name": graph_name,
            "from_graph_name": from_G.name(),
            "config": config,
        }

        result = self._query_runner.run_query_with_logging(query, params).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result
