from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import (
    ShortestPathEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_endpoints import (
    SourceTargetDijkstraEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.source_target_dijkstra_arrow_endpoints import (
    SourceTargetDijkstraArrowEndpoints,
)


class ShortestPathArrowEndpoints(ShortestPathEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._dijkstra = SourceTargetDijkstraArrowEndpoints(arrow_client, write_back_client, show_progress)

    @property
    def dijkstra(self) -> SourceTargetDijkstraEndpoints:
        return self._dijkstra
