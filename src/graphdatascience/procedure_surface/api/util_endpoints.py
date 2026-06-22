from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from neo4j.graph import Node

from graphdatascience.graph.graph_api import Graph


class UtilEndpoints(ABC):
    @abstractmethod
    def as_node(self, node_id: int) -> Node:
        """
        Get a node from a node id.

        Parameters
        ----------
        node_id: int
            The id of the node to get.

        Returns
        -------
        Node
            The node with the given id.
        """
        pass

    @abstractmethod
    def as_nodes(self, node_ids: list[int]) -> list[Node]:
        """
        Get a list of nodes from a list of node ids.

        Parameters
        ----------
        node_ids: list[int]
            The ids of the nodes to get.

        Returns
        -------
        list[Node]
            The nodes with the given ids.
        """
        pass

    @abstractmethod
    def node_property(self, G: Graph, node_id: int, property_key: str, node_label: str = "*") -> Any:
        """
        Get the property of a node with the given id from an in-memory graph.

        Not available in AGA sessions; stream the node properties once
        (``gds.graph.node_properties.stream``) and filter on the client side instead.

        Parameters
        ----------
        G: Graph
            Graph object to use
        node_id: int
            The id of the node to get the property from.
        property_key: str
            The key of the property to get.
        node_label: str, default="*"
            The label of the node to get the property from.

        Returns
        -------
        Any
            The property of the node with the given id.
        """
        pass

    def one_hot_encoding(self, available_values: list[Any] | None, selected_values: list[Any] | None) -> list[int]:
        """
        One hot encode a list of values.

        Parameters
        ----------
        available_values: list[Any] | None
            The available values to encode.
        selected_values: list[Any] | None
            The selected values.

        Returns
        -------
        list[int]
            The one hot encoded values.
        """
        if available_values is None:
            return []
        if selected_values is None:
            return [0] * len(available_values)
        selected = set(selected_values)
        return [1 if value in selected else 0 for value in available_values]
