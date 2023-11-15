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
        result = self._query_runner.call_function(endpoint=self._namespace, body=str(node_id))

        return result[0].squeeze()

    def asNodes(self, node_ids: List[int]) -> List[Any]:
        """
        Get a list of nodes from a list of node ids.

        Args:
            node_ids: The ids of the nodes to get.

        Returns:
            The nodes with the given ids.

        """
        self._namespace += ".asNodes"
        result = self._query_runner.call_function(
            endpoint=self._namespace, body="$node_ids", params={"node_ids": node_ids}
        )

        return result[0].squeeze()  # type: ignore

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

        body = "$graph_name, $node_id, $property_key, $node_label"
        params = {
            "graph_name": G.name(),
            "node_id": node_id,
            "property_key": property_key,
            "node_label": node_label,
        }
        result = self._query_runner.call_function(endpoint=self._namespace, body=body, params=params)

        return result[0].squeeze()
