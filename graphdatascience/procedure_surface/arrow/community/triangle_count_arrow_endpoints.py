from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.triangle_count_endpoints import (
    TriangleCountEndpoints,
    TriangleCountMutateResult,
    TriangleCountStatsResult,
    TriangleCountWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class TriangleCountArrowEndpoints(TriangleCountEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client, show_progress)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        label_filter: Optional[List[str]] = None,
        log_progress: bool = True,
        max_degree: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> TriangleCountMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
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

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/community.triangleCount", G, config, mutate_property
        )

        return TriangleCountMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        label_filter: Optional[List[str]] = None,
        log_progress: bool = True,
        max_degree: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> TriangleCountStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/community.triangleCount", G, config
        )

        return TriangleCountStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        label_filter: Optional[List[str]] = None,
        log_progress: bool = True,
        max_degree: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
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

        return self._node_property_endpoints.run_job_and_stream("v2/community.triangleCount", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        label_filter: Optional[List[str]] = None,
        log_progress: bool = True,
        max_degree: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> TriangleCountWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
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
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.triangleCount", G, config, write_concurrency, concurrency, write_property
        )

        return TriangleCountWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = None,
        label_filter: Optional[List[str]] = None,
        max_degree: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            concurrency=concurrency,
            label_filter=label_filter,
            max_degree=max_degree,
            node_labels=node_labels,
            relationship_types=relationship_types,
        )

        return self._node_property_endpoints.estimate("v2/community.triangleCount.estimate", G, config)
