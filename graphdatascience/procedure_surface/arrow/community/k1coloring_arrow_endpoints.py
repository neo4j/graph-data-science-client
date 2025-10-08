from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.k1coloring_endpoints import (
    K1ColoringEndpoints,
    K1ColoringMutateResult,
    K1ColoringStatsResult,
    K1ColoringWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class K1ColoringArrowEndpoints(K1ColoringEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> K1ColoringMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.k1coloring", G, config, mutate_property)

        return K1ColoringMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> K1ColoringStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.k1coloring", G, config)

        return K1ColoringStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        min_community_size: Optional[int] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.k1coloring", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        min_community_size: Optional[int] = None,
    ) -> K1ColoringWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.k1coloring", G, config, write_concurrency, concurrency, write_property
        )

        return K1ColoringWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            batch_size=batch_size,
            max_iterations=max_iterations,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )
        return self._node_property_endpoints.estimate("v2/community.k1coloring.estimate", G, algo_config=config)
