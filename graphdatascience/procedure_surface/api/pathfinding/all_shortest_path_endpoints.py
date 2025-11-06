from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_endpoints import SingleSourceDeltaEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_dijkstra_endpoints import (
    SingleSourceDijkstraEndpoints,
)


class AllShortestPathEndpoints(ABC):
    """
    Container for shortest path algorithm endpoints from a given source node.
    Provides access to different shortest path algorithms such as Delta Stepping, Dijkstra, and Bellman-Ford.
    """

    @property
    @abstractmethod
    def delta(self) -> SingleSourceDeltaEndpoints:
        """Access to Delta Stepping shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def dijkstra(self) -> SingleSourceDijkstraEndpoints:
        """Access to Dijkstra shortest path algorithm endpoints."""
        ...
