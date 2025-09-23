from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.pagerank_endpoints import (
    PageRankEndpoints,
    PageRankMutateResult,
    PageRankStatsResult,
    PageRankWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class PageRankArrowEndpoints(PageRankEndpoints):
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
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> PageRankMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.pageRank", G, config, mutate_property)

        return PageRankMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> PageRankStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.pageRank", G, config)

        return PageRankStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.pageRank", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
    ) -> PageRankWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.pageRank", G, config, write_concurrency, concurrency, write_property
        )
        return PageRankWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            damping_factor=damping_factor,
            tolerance=tolerance,
            max_iterations=max_iterations,
            scaler=scaler,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
            source_nodes=source_nodes,
        )
        return self._node_property_endpoints.estimate("v2/centrality.pageRank.estimate", G, config)
