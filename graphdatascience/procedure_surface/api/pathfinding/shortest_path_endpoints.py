from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_endpoints import (
    SourceTargetDijkstraEndpoints,
)


class ShortestPathEndpoints(ABC):
    """
    Container for shortest path algorithm endpoints.
    Provides access to different shortest path algorithms like Dijkstra.
    """

    @property
    @abstractmethod
    def dijkstra(self) -> SourceTargetDijkstraEndpoints:
        """Access to Dijkstra shortest path algorithm endpoints."""
        ...
