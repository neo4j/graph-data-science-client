from typing import Any, List, Optional, Union

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
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class SllpaArrowEndpoints(SllpaEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = False,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.sllpa", G, config, mutate_property)

        return SllpaMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.sllpa", G, config)

        return SllpaStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
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
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
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
            "v2/community.sllpa", G, config, write_concurrency, concurrency, write_property
        )

        return SllpaWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
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
