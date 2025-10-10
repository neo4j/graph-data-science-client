from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.wcc_endpoints import (
    WccEndpoints,
    WccMutateResult,
    WccStatsResult,
    WccWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class WccArrowEndpoints(WccEndpoints):
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
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> WccMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.wcc", G, config, mutate_property)

        return WccMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> WccStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.wcc", G, config)

        return WccStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        min_component_size: int | None = None,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.wcc", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        min_component_size: int | None = None,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
        write_concurrency: int | None = None,
    ) -> WccWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.wcc", G, config, write_concurrency, concurrency, write_property
        )

        return WccWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            threshold=threshold,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            seed_property=seed_property,
            consecutive_ids=consecutive_ids,
            relationship_weight_property=relationship_weight_property,
        )
        return self._node_property_endpoints.estimate("v2/community.wcc.estimate", G, config)
