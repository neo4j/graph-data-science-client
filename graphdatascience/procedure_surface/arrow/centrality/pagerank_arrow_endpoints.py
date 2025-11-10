from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.centrality.pagerank_endpoints import (
    PageRankEndpoints,
    PageRankMutateResult,
    PageRankStatsResult,
    PageRankWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class PageRankArrowEndpoints(PageRankEndpoints):
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
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
    ) -> PageRankMutateResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

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
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.pageRank", config, mutate_property)

        return PageRankMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
    ) -> PageRankStatsResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

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
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.pageRank", config)

        return PageRankStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
    ) -> DataFrame:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

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
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.pageRank", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
        write_concurrency: int | None = None,
    ) -> PageRankWriteResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

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
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.pageRank",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )
        return PageRankWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
    ) -> EstimationResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_estimate_config(
            damping_factor=damping_factor,
            tolerance=tolerance,
            max_iterations=max_iterations,
            scaler=scaler_value,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
            source_nodes=source_nodes,
        )
        return self._node_property_endpoints.estimate("v2/centrality.pageRank.estimate", G, config)
