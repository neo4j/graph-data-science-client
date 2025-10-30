from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_endpoints import (
    SourceTargetAStarEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_endpoints import (
    SourceTargetDijkstraEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_yens_endpoints import (
    SourceTargetYensEndpoints,
)


class ShortestPathEndpoints(ABC):
    """
    Container for shortest path algorithm endpoints.
    Provides access to different shortest path algorithms such as Dijkstra and A*.
    """

    @property
    @abstractmethod
    def dijkstra(self) -> SourceTargetDijkstraEndpoints:
        """Access to Dijkstra shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def astar(self) -> SourceTargetAStarEndpoints:
        """Access to A* shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def yens(self) -> SourceTargetYensEndpoints:
        """Access to Yen's K shortest paths algorithm endpoints."""
        ...
