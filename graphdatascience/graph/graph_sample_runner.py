from typing import Any, Tuple

from pandas import Series

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_object import Graph
from .graph_type_check import from_graph_type_check


class GraphSampleRunner(CallerBase, IllegalAttrChecker):
    @client_only_endpoint("gds.alpha.graph.sample")
    @compatible_with("construct", min_inclusive=ServerVersion(2, 2, 0))
    @from_graph_type_check
    def rwr(self, graph_name: str, from_G: Graph, **config: Any) -> Tuple[Graph, "Series[Any]"]:
        self._namespace += ".rwr"

        query = f"CALL {self._namespace}($graph_name, $from_graph_name, $config)"
        params = {
            "graph_name": graph_name,
            "from_graph_name": from_G.name(),
            "config": config,
        }

        result = self._query_runner.run_query_with_logging(query, params).squeeze()

        return Graph(graph_name, self._query_runner, self._server_version), result
