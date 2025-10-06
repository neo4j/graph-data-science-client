from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import (
    LabelPropagationEndpoints,
    LabelPropagationMutateResult,
    LabelPropagationStatsResult,
    LabelPropagationWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class LabelPropagationArrowEndpoints(LabelPropagationEndpoints):
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
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> LabelPropagationMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/community.labelPropagation", G, config, mutate_property
        )

        return LabelPropagationMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> LabelPropagationStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/community.labelPropagation", G, config
        )

        return LabelPropagationStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.labelPropagation", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> LabelPropagationWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.labelPropagation", G, config, write_concurrency, concurrency, write_property
        )

        return LabelPropagationWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            max_iterations=max_iterations,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
        )
        return self._node_property_endpoints.estimate("v2/community.labelPropagation.estimate", G, config)
