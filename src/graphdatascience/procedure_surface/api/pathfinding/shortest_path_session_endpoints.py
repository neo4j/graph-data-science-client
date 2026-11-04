from __future__ import annotations

from abc import abstractmethod

from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import ShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_session_endpoints import (
    SourceTargetAStarSessionEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_session_endpoints import (
    SourceTargetDijkstraSessionEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_yens_session_endpoints import (
    SourceTargetYensSessionEndpoints,
)


class ShortestPathSessionEndpoints(ShortestPathEndpoints):
    """Container for shortest path algorithm endpoints as exposed by a GDS Session."""

    @property
    @abstractmethod
    def dijkstra(self) -> SourceTargetDijkstraSessionEndpoints:
        """Access to Dijkstra shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def a_star(self) -> SourceTargetAStarSessionEndpoints:
        """Access to A* shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def yens(self) -> SourceTargetYensSessionEndpoints:
        """Access to Yen's K shortest paths algorithm endpoints."""
        ...
