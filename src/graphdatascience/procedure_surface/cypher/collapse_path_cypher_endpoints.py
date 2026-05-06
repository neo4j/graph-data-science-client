from __future__ import annotations

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import CollapsePathResult
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS
from graphdatascience.procedure_surface.api.collapse_path_endpoints import CollapsePathEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class CollapsePathCypherEndpoints(CollapsePathEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.collapsePath.mutate", params=params).squeeze()
        return CollapsePathResult(**result.to_dict())
