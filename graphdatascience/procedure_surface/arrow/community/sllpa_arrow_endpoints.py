from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import (
    SllpaEndpoints,
    SllpaMutateResult,
    SllpaStatsResult,
    SllpaWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class SllpaArrowEndpoints(SllpaEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> SllpaMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.sllpa", config, mutate_property)

        return SllpaMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> SllpaStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.sllpa", config)

        return SllpaStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.sllpa", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> SllpaWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.sllpa",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return SllpaWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        max_iterations: int,
        concurrency: int | None = None,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            max_iterations=max_iterations,
            concurrency=concurrency,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
        )

        return self._node_property_endpoints.estimate("v2/community.sllpa.estimate", G, config)
