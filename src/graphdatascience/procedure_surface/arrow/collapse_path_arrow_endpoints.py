from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import CollapsePathResult
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS
from graphdatascience.procedure_surface.api.collapse_path_endpoints import CollapsePathEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class CollapsePathArrowEndpoints(CollapsePathEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = False):
        self._arrow_client = arrow_client
        self._show_progress = show_progress

    def mutate(
        self,
        G: GraphV2,
        path_templates: list[list[str]],
        mutate_relationship_type: str,
        *,
        node_labels: list[str] = ALL_LABELS,
        allow_self_loops: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> CollapsePathResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            path_templates=path_templates,
            mutate_relationship_type=mutate_relationship_type,
            node_labels=node_labels,
            allow_self_loops=allow_self_loops,
            concurrency=concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        show_progress = self._show_progress and log_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.relationships.collapsePath", config, show_progress=show_progress
        )

        return CollapsePathResult(**JobClient.get_summary(self._arrow_client, job_id))
