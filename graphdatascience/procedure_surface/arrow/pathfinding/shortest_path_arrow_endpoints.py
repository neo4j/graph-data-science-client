from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import (
    ShortestPathEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_endpoints import (
    SourceTargetAStarEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_endpoints import (
    SourceTargetDijkstraEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_yens_endpoints import (
    SourceTargetYensEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.source_target_astar_arrow_endpoints import (
    AStarArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.source_target_dijkstra_arrow_endpoints import (
    SourceTargetDijkstraArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.source_target_yens_arrow_endpoints import (
    YensArrowEndpoints,
)


class ShortestPathArrowEndpoints(ShortestPathEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._dijkstra = SourceTargetDijkstraArrowEndpoints(arrow_client, write_back_client, show_progress)
        self._astar = AStarArrowEndpoints(arrow_client, write_back_client, show_progress)
        self._yens = YensArrowEndpoints(arrow_client, write_back_client, show_progress)

    @property
    def dijkstra(self) -> SourceTargetDijkstraEndpoints:
        return self._dijkstra

    @property
    def astar(self) -> SourceTargetAStarEndpoints:
        return self._astar

    @property
    def yens(self) -> SourceTargetYensEndpoints:
        return self._yens
