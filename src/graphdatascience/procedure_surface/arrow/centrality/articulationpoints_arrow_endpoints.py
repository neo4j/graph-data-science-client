from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.articulationpoints_endpoints import (
    ArticulationPointsEndpoints,
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
    ArticulationPointsWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class ArticulationPointsArrowEndpoints(ArticulationPointsEndpoints):
    """Arrow-based implementation of ArticulationPoints algorithm endpoints."""

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: Graph,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )
        return self._node_property_endpoints.run_job(G, "v2/centrality.articulationPoints", config)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ArticulationPointsMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/centrality.articulationPoints", config, mutate_property
        )

        return ArticulationPointsMutateResult(**result)

    def stats(
        self,
        G: Graph,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ArticulationPointsStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/centrality.articulationPoints", config
        )

        return ArticulationPointsStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        raise NotImplementedError(
            "Stream mode is not supported for ArticulationPoints arrow endpoints. "
            "The result columns cannot be preserved with the current implementation. "
            "Use cypher endpoints for stream functionality."
        )

    def write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> ArticulationPointsWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.articulationPoints",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return ArticulationPointsWriteResult(**result)

    def estimate(
        self,
        G: Graph | dict[str, Any],
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )
        return self._node_property_endpoints.estimate("v2/centrality.articulationPoints.estimate", G, config)
