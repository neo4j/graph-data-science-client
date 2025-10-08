from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationEndpoints,
    ModularityOptimizationMutateResult,
    ModularityOptimizationStatsResult,
    ModularityOptimizationWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class ModularityOptimizationArrowEndpoints(ModularityOptimizationEndpoints):
    """
    Arrow client implementation for the Modularity Optimization algorithm.
    """

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
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
    ) -> ModularityOptimizationMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/community.modularityOptimization", G, config, mutate_property
        )

        return ModularityOptimizationMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
    ) -> ModularityOptimizationStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_get_summary("v2/community.modularityOptimization", G, config)

        return ModularityOptimizationStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.modularityOptimization", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> ModularityOptimizationWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
            write_concurrency=write_concurrency,
            write_to_result_store=write_to_result_store,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.modularityOptimization", G, config, write_concurrency, concurrency, write_property
        )

        return ModularityOptimizationWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        tolerance: Optional[float] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            max_iterations=max_iterations,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            tolerance=tolerance,
        )

        return self._node_property_endpoints.estimate("v2/community.modularityOptimization.estimate", G, config)
