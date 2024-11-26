from typing import Any

from ..error.cypher_warning_handler import (
    filter_id_func_deprecation_warning,
)
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..utils.util_node_property_func_runner import NodePropertyFuncRunner


class UtilRemoteProcRunner(UncallableNamespace, IllegalAttrChecker):
    @filter_id_func_deprecation_warning()
    def asNode(self, node_id: int) -> Any:
        """
        Get a node from a node id.

        Args:
            node_id: The id of the node to get.

        Returns:
            The node with the given id.

        """
        query = "MATCH (n) WHERE id(n) = $nodeId RETURN n"
        params = {"nodeId": node_id}

        return self._query_runner.run_cypher(query=query, params=params).squeeze()

    @filter_id_func_deprecation_warning()
    def asNodes(self, node_ids: list[int]) -> list[Any]:
        """
        Get a list of nodes from a list of node ids.

        Args:
            node_ids: The ids of the nodes to get.

        Returns:
            The nodes with the given ids.

        """
        query = "MATCH (n) WHERE id(n) IN $nodeIds RETURN collect(n)"
        params = {"nodeIds": node_ids}

        return self._query_runner.run_cypher(query=query, params=params).squeeze()  # type: ignore

    @property
    def nodeProperty(self) -> NodePropertyFuncRunner:
        return NodePropertyFuncRunner(self._query_runner, self._namespace + ".nodeProperty", self._server_version)
