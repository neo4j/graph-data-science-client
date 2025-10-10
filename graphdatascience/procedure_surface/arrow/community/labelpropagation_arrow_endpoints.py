from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import (
    LabelPropagationEndpoints,
    LabelPropagationMutateResult,
    LabelPropagationStatsResult,
    LabelPropagationWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class LabelPropagationArrowEndpoints(LabelPropagationEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        node_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        node_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        min_community_size: int | None = None,
        node_labels: list[str] | None = None,
        node_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        min_community_size: int | None = None,
        node_labels: list[str] | None = None,
        node_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: int | None = None,
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
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        node_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
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
