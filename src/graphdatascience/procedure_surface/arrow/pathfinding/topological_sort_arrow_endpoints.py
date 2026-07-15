from __future__ import annotations

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.pathfinding.topological_sort_endpoints import TopologicalSortEndpoints
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import map_topological_sort_stream_result


class TopologicalSortArrowEndpoints(TopologicalSortEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        show_progress: bool = False,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol=None, show_progress=show_progress
        )

    def stream(
        self,
        G: Graph,
        *,
        compute_max_distance_from_source: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            computeMaxDistanceFromSource=compute_max_distance_from_source,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._node_property_endpoints.run_job_and_stream("v2/pathfinding.topologicalSort", G, config)
        map_topological_sort_stream_result(result)

        return result
