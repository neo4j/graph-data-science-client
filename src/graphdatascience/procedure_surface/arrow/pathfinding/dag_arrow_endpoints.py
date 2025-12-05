from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.pathfinding.dag_endpoints import DagEndpoints
from graphdatascience.procedure_surface.api.pathfinding.longest_path_endpoints import LongestPathEndpoints
from graphdatascience.procedure_surface.arrow.pathfinding.longest_path_arrow_endpoints import (
    LongestPathArrowEndpoints,
)


class DagArrowEndpoints(DagEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        show_progress: bool = False,
    ):
        self._longest_path = LongestPathArrowEndpoints(arrow_client, show_progress)

    @property
    def longest_path(self) -> LongestPathEndpoints:
        return self._longest_path
