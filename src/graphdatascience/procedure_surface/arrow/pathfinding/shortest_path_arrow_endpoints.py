from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.pathfinding.shortest_path_session_endpoints import (
    ShortestPathSessionEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_session_endpoints import (
    SourceTargetAStarSessionEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_session_endpoints import (
    SourceTargetDijkstraSessionEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_yens_session_endpoints import (
    SourceTargetYensSessionEndpoints,
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
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class ShortestPathArrowEndpoints(ShortestPathSessionEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._dijkstra = SourceTargetDijkstraArrowEndpoints(arrow_client, write_protocol, show_progress)
        self._astar = AStarArrowEndpoints(arrow_client, write_protocol, show_progress)
        self._yens = YensArrowEndpoints(arrow_client, write_protocol, show_progress)

    @property
    def dijkstra(self) -> SourceTargetDijkstraSessionEndpoints:
        return self._dijkstra

    @property
    def a_star(self) -> SourceTargetAStarSessionEndpoints:
        return self._astar

    @property
    def yens(self) -> SourceTargetYensSessionEndpoints:
        return self._yens
