from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.triangle_count_endpoints import (
    TriangleCountEndpoints,
    TriangleCountMutateResult,
    TriangleCountStatsResult,
    TriangleCountWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class TriangleCountCypherEndpoints(TriangleCountEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        label_filter: list[str] | None = None,
        log_progress: bool = True,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> TriangleCountMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            label_filter=label_filter,
            log_progress=log_progress,
            max_degree=max_degree,
            mutate_property=mutate_property,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.triangleCount.mutate", params=params).squeeze()

        return TriangleCountMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        label_filter: list[str] | None = None,
        log_progress: bool = True,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> TriangleCountStatsResult:
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.triangleCount.stats", params=params).squeeze()

        return TriangleCountStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        label_filter: list[str] | None = None,
        log_progress: bool = True,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.triangleCount.stream", params=params)

        return result

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        label_filter: list[str] | None = None,
        log_progress: bool = True,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> TriangleCountWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            label_filter=label_filter,
            log_progress=log_progress,
            max_degree=max_degree,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
            write_property=write_property,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.triangleCount.write", params=params).squeeze()

        return TriangleCountWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        label_filter: list[str] | None = None,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            label_filter=label_filter,
            max_degree=max_degree,
            node_labels=node_labels,
            relationship_types=relationship_types,
        )

        return estimate_algorithm("gds.triangleCount.stream.estimate", self._query_runner, G, config)
