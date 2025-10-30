from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import AllShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_endpoints import SingleSourceDeltaEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.single_source_delta_arrow_endpoints import (
    DeltaSteppingArrowEndpoints,
)


class AllShortestPathArrowEndpoints(AllShortestPathEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._delta = DeltaSteppingArrowEndpoints(arrow_client, write_back_client, show_progress)

    @property
    def delta(self) -> SingleSourceDeltaEndpoints:
        return self._delta
