from __future__ import annotations

from neo4j.graph import Node

from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import (
    Direction,
    TopologicalLinkPredictionEndpoints,
)

_NOT_IMPLEMENTED_MESSAGE = "Topological link prediction functions are not available in AGA sessions."


class TopologicalLinkPredictionArrowEndpoints(TopologicalLinkPredictionEndpoints):
    def adamic_adar(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)

    def common_neighbors(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)

    def preferential_attachment(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)

    def resource_allocation(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)

    def same_community(
        self,
        node1: int | Node,
        node2: int | Node,
        community_property: str = "community",
    ) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)

    def total_neighbors(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)
