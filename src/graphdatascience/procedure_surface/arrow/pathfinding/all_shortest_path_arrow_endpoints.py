from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import AllShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_endpoints import SingleSourceDeltaEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_dijkstra_endpoints import (
    SingleSourceDijkstraEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.single_source_bellman_ford_arrow_endpoints import (
    BellmanFordArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.single_source_delta_arrow_endpoints import (
    DeltaSteppingArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pathfinding.single_source_dijkstra_arrow_endpoints import (
    SingleSourceDijkstraArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.stream_result_mapper import map_all_shortest_path_stream_result
from graphdatascience.procedure_surface.arrow.table_endpoints_helper import TableEndpointsHelper


class AllShortestPathArrowEndpoints(AllShortestPathEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._delta = DeltaSteppingArrowEndpoints(arrow_client, write_back_client, show_progress)
        self._dijkstra = SingleSourceDijkstraArrowEndpoints(arrow_client, write_back_client, show_progress)
        self._bellman_ford = BellmanFordArrowEndpoints(arrow_client, write_back_client, show_progress)
        self._endpoint_helper = TableEndpointsHelper(arrow_client, show_progress=show_progress)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = self._endpoint_helper.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            relationship_weight_property=relationship_weight_property,
        )

        result = self._endpoint_helper.run_job_and_stream("v2/pathfinding.allShortestPaths", G, config)
        map_all_shortest_path_stream_result(result)
        return result

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoint_helper.create_estimate_config(
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return self._endpoint_helper.estimate("v2/pathfinding.allShortestPaths.estimate", G, config)

    @property
    def delta(self) -> SingleSourceDeltaEndpoints:
        return self._delta

    @property
    def dijkstra(self) -> SingleSourceDijkstraEndpoints:
        return self._dijkstra
