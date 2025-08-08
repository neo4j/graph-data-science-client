from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.articlerank_endpoints import (
    ArticleRankEndpoints,
    ArticleRankMutateResult,
    ArticleRankStatsResult,
    ArticleRankWriteResult,
)
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class ArticleRankArrowEndpoints(ArticleRankEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> ArticleRankMutateResult:
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

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/centrality.articleRank", G, config, mutate_property
        )

        return ArticleRankMutateResult(**result)

    def stats(
        self,
        G: Graph,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> ArticleRankStatsResult:
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/centrality.articleRank", G, config
        )

        return ArticleRankStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.articleRank", G, config)

    def write(
        self,
        G: Graph,
        write_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
    ) -> ArticleRankWriteResult:
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
            "v2/centrality.articleRank", G, config, write_concurrency, concurrency
        )

        return ArticleRankWriteResult(**result)

    def estimate(self, G: Union[Graph, dict[str, Any]]) -> EstimationResult:
        return self._node_property_endpoints.estimate("v2/centrality.articleRank.estimate", G)
