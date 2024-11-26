from typing import Any

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..utils.util_node_property_func_runner import NodePropertyFuncRunner


class UtilProcRunner(UncallableNamespace, IllegalAttrChecker):
    def asNode(self, node_id: int) -> Any:
        """
        Get a node from a node id.

        Args:
            node_id: The id of the node to get.

        Returns:
            The node with the given id.

        """
        self._namespace += ".asNode"
        result = self._query_runner.run_cypher(f"RETURN {self._namespace}({node_id}) AS node")

        return result.iat[0, 0]

    def asNodes(self, node_ids: list[int]) -> list[Any]:
        """
        Get a list of nodes from a list of node ids.

        Args:
            node_ids: The ids of the nodes to get.

        Returns:
            The nodes with the given ids.

        """
        self._namespace += ".asNodes"
        result = self._query_runner.run_cypher(f"RETURN {self._namespace}({node_ids}) AS nodes")

        return result.iat[0, 0]  # type: ignore

    @property
    def nodeProperty(self) -> NodePropertyFuncRunner:
        return NodePropertyFuncRunner(self._query_runner, self._namespace + ".nodeProperty", self._server_version)
