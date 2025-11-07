from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.eigenvector_endpoints import (
    EigenvectorEndpoints,
    EigenvectorMutateResult,
    EigenvectorStatsResult,
    EigenvectorWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class EigenvectorArrowEndpoints(EigenvectorEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.eigenvector", config, mutate_property)

        return EigenvectorMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.eigenvector", config)

        return EigenvectorStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: Any | None = None,
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
            "v2/centrality.eigenvector",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return EigenvectorWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: Any | None = None,
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
