from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum

from neo4j.graph import Node


class Direction(str, Enum):
    """The direction of relationships to consider when computing a topological link prediction."""

    OUTGOING = "OUTGOING"
    INCOMING = "INCOMING"
    BOTH = "BOTH"


class TopologicalLinkPredictionEndpoints(ABC):
    @abstractmethod
    def adamic_adar(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        """
        Compute the Adamic-Adar index for two nodes.

        Parameters
        ----------
        node1: int | Node
            The first node, either as a node id or a node object.
        node2: int | Node
            The second node, either as a node id or a node object.
        relationship_query: str | None, default=None
            The relationship type used to compute similarity between node1 and node2
        direction: Direction, default=Direction.BOTH
            The relationship direction used to compute similarity between node1 and node2

        Returns
        -------
        float
            The Adamic-Adar index.
        """
        pass

    @abstractmethod
    def common_neighbors(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        """
        Compute the number of common neighbors for two nodes.

        Parameters
        ----------
        node1: int | Node
            The first node, either as a node id or a node object.
        node2: int | Node
            The second node, either as a node id or a node object.
        relationship_query: str | None, default=None
            The relationship type used to compute similarity between node1 and node2The relationship type used to compute similarity between node1 and node2
        direction: Direction, default=Direction.BOTH
            The relationship direction used to compute similarity between node1 and node2

        Returns
        -------
        float
            The number of common neighbors.
        """
        pass

    @abstractmethod
    def preferential_attachment(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        """
        Compute the preferential attachment score for two nodes.

        Parameters
        ----------
        node1: int | Node
            The first node, either as a node id or a node object.
        node2: int | Node
            The second node, either as a node id or a node object.
        relationship_query: str | None, default=None
            The relationship type used to compute similarity between node1 and node2
        direction: Direction, default=Direction.BOTH
            The relationship direction used to compute similarity between node1 and node2

        Returns
        -------
        float
            The preferential attachment score.
        """
        pass

    @abstractmethod
    def resource_allocation(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        """
        Compute the resource allocation index for two nodes.

        Parameters
        ----------
        node1: int | Node
            The first node, either as a node id or a node object.
        node2: int | Node
            The second node, either as a node id or a node object.
        relationship_query: str | None, default=None
            The relationship type used to compute similarity between node1 and node2
        direction: Direction, default=Direction.BOTH
            The relationship direction used to compute similarity between node1 and node2

        Returns
        -------
        float
            The resource allocation index.
        """
        pass

    @abstractmethod
    def same_community(
        self,
        node1: int | Node,
        node2: int | Node,
        community_property: str = "community",
    ) -> float:
        """
        Determine whether two nodes belong to the same community.

        Parameters
        ----------
        node1: int | Node
            The first node, either as a node id or a node object.
        node2: int | Node
            The second node, either as a node id or a node object.
        community_property: str, default="community"
            The node property holding the community id.

        Returns
        -------
        float
            ``1.0`` if both nodes are in the same community, ``0.0`` otherwise.
        """
        pass

    @abstractmethod
    def total_neighbors(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        """
        Compute the total number of neighbors for two nodes.

        Parameters
        ----------
        node1: int | Node
            The first node, either as a node id or a node object.
        node2: int | Node
            The second node, either as a node id or a node object.
        relationship_query: str | None, default=None
            The relationship type used to compute similarity between node1 and node2
        direction: Direction, default=Direction.BOTH
            The relationship direction used to compute similarity between node1 and node2

        Returns
        -------
        float
            The total number of neighbors.
        """
        pass
