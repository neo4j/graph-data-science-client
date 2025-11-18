from typing import Any

from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion

from ..error.cypher_warning_handler import (
    filter_id_func_deprecation_warning,
)
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class TopologicalLPRunner(UncallableNamespace, IllegalAttrChecker):
    @filter_id_func_deprecation_warning()
    def _run_standard_function(self, node1: int, node2: int, config: dict[str, Any]) -> float:
        query = f"""
        MATCH (n1) WHERE id(n1) = {node1}
        MATCH (n2) WHERE id(n2) = {node2}
        RETURN {self._namespace}(n1, n2, $config) AS score
        """
        params = {"config": config}
        return self._query_runner.run_retryable_cypher(query, params, mode=QueryMode.READ)["score"].squeeze()  # type: ignore

    @compatible_with("adamicAdar", min_inclusive=ServerVersion(2, 24, 0))
    def adamicAdar(self, node1: int, node2: int, **config: Any) -> float:
        self._namespace += ".adamicAdar"
        return self._run_standard_function(node1, node2, config)

    @compatible_with("resourceAllocation", min_inclusive=ServerVersion(2, 24, 0))
    def resourceAllocation(self, node1: int, node2: int, **config: Any) -> float:
        self._namespace += ".resourceAllocation"
        return self._run_standard_function(node1, node2, config)

    @compatible_with("commonNeighbors", min_inclusive=ServerVersion(2, 24, 0))
    def commonNeighbors(self, node1: int, node2: int, **config: Any) -> float:
        self._namespace += ".commonNeighbors"
        return self._run_standard_function(node1, node2, config)

    @compatible_with("totalNeighbors", min_inclusive=ServerVersion(2, 24, 0))
    def totalNeighbors(self, node1: int, node2: int, **config: Any) -> float:
        self._namespace += ".totalNeighbors"
        return self._run_standard_function(node1, node2, config)
