from __future__ import annotations

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.triangles_endpoints import TrianglesEndpoints
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class TrianglesArrowEndpoints(TrianglesEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = True):
        self._arrow_client = arrow_client
        self._show_progress = show_progress

    def __call__(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        label_filter: list[str] | None = None,
        log_progress: bool = True,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            job_id=job_id,
            label_filter=label_filter,
            log_progress=log_progress,
            max_degree=max_degree,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )
        show_progress = config.get("logProgress", True) and self._show_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/community.triangles", config, show_progress=show_progress
        )
        return JobClient.stream_results(self._arrow_client, G.name(), job_id)
