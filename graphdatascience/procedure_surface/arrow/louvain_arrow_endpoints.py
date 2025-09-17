from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from ..api.louvain_endpoints import LouvainEndpoints, LouvainMutateResult, LouvainStatsResult, LouvainWriteResult
from .node_property_endpoints import NodePropertyEndpoints


class LouvainArrowEndpoints(LouvainEndpoints):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> LouvainMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.louvain", G, config, mutate_property)

        return LouvainMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> LouvainStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.louvain", G, config)

        return LouvainStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        min_community_size: Optional[int] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.louvain", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        min_community_size: Optional[int] = None,
    ) -> LouvainWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            write_concurrency=write_concurrency,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.louvain", G, config, write_concurrency, concurrency
        )

        return LouvainWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            tolerance=tolerance,
            max_levels=max_levels,
            include_intermediate_communities=include_intermediate_communities,
            max_iterations=max_iterations,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            seed_property=seed_property,
            consecutive_ids=consecutive_ids,
            relationship_weight_property=relationship_weight_property,
        )
        return self._node_property_endpoints.estimate("v2/community.louvain.estimate", G, config)
