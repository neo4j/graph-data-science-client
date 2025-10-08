from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.leiden_endpoints import (
    LeidenEndpoints,
    LeidenMutateResult,
    LeidenStatsResult,
    LeidenWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class LeidenArrowEndpoints(LeidenEndpoints):
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
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> LeidenMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.leiden", G, config, mutate_property)

        return LeidenMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> LeidenStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.leiden", G, config)

        return LeidenStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.leiden", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
    ) -> LeidenWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.leiden", G, config, write_concurrency, concurrency, write_property
        )

        return LeidenWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[Any] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            max_levels=max_levels,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            theta=theta,
            tolerance=tolerance,
        )
        return self._node_property_endpoints.estimate("v2/community.leiden.estimate", G, config)
