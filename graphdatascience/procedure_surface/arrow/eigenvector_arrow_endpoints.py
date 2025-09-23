from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.eigenvector_endpoints import (
    EigenvectorEndpoints,
    EigenvectorMutateResult,
    EigenvectorStatsResult,
    EigenvectorWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class EigenvectorArrowEndpoints(EigenvectorEndpoints):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> EigenvectorMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
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
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/centrality.eigenvector", G, config, mutate_property
        )

        return EigenvectorMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> EigenvectorStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
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
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/centrality.eigenvector", G, config
        )

        return EigenvectorStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
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
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.eigenvector", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> EigenvectorWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
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
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.eigenvector", G, config, write_concurrency, concurrency, write_property
        )

        return EigenvectorWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        algo_config = self._node_property_endpoints.create_estimate_config(
            max_iterations=max_iterations,
            tolerance=tolerance,
            source_nodes=source_nodes,
            scaler=scaler,
            relationship_weight_property=relationship_weight_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )

        return self._node_property_endpoints.estimate("v2/centrality.eigenvector.estimate", G, algo_config)
