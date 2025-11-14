from typing import Any

from graphdatascience.call_parameters import CallParameters

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
        # TODO use call_function?
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
        # TODO use call_function?
        result = self._query_runner.run_cypher(f"RETURN {self._namespace}({node_ids}) AS nodes")

        return result.iat[0, 0]  # type: ignore

    @property
    def nodeProperty(self) -> NodePropertyFuncRunner:
        return NodePropertyFuncRunner(self._query_runner, self._namespace + ".nodeProperty", self._server_version)

    def oneHotEncoding(self, available_values: list[Any], selected_values: list[Any]) -> list[int]:
        """
        One hot encode a list of values.

        Args:
            available_values: The available values to encode.
            selected_values: The values to encode.

        Returns:
            The one hot encoded values.
        """
        namespace = self._namespace + ".oneHotEncoding"

        params = CallParameters(
            available_values=available_values,
            selected_values=selected_values,
        )
        query = f"RETURN {namespace}($available_values, $selected_values) AS encoded"
        result = self._query_runner.run_cypher(query=query, params=params)

        return result.iat[0, 0]  # type: ignore
