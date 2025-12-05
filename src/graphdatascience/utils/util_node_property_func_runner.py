from typing import Any

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check


class NodePropertyFuncRunner(IllegalAttrChecker):
    @graph_type_check
    def __call__(self, G: Graph, node_id: int, property_key: str, node_label: str = "*") -> Any:
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
        params = CallParameters(
            graph_name=G.name(),
            node_id=node_id,
            property_key=property_key,
            node_label=node_label,
        )
        return self._query_runner.call_function(endpoint=self._namespace, params=params)
