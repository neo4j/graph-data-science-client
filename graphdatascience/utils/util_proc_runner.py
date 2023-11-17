from typing import Any, List

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check


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

    def asNodes(self, node_ids: List[int]) -> List[Any]:
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

    @graph_type_check
    def nodeProperty(self, G: Graph, node_id: int, property_key: str, node_label: str = "*") -> Any:
        """
        Get the property of a node with the given id.

        Args:
            G: The graph to get the node property from.
            node_id: The id of the node to get the property from.
            property_key: The key of the property to get.
            node_label: The label of the node to get the property from.

        Returns:
            The property of the node with the given id.

        """
        self._namespace += ".nodeProperty"

        query = f"RETURN {self._namespace}($graph_name, $node_id, $property_key, $node_label) as property"
        params = {
            "graph_name": G.name(),
            "node_id": node_id,
            "property_key": property_key,
            "node_label": node_label,
        }
        result = self._query_runner.run_cypher(query, params)

        return result.iat[0, 0]
