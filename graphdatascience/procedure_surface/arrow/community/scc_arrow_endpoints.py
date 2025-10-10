from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.scc_endpoints import (
    SccEndpoints,
    SccMutateResult,
    SccStatsResult,
    SccWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class SccArrowEndpoints(SccEndpoints):
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
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        consecutive_ids: bool | None = None,
    ) -> SccMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.scc", G, config, mutate_property)

        return SccMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        consecutive_ids: bool | None = None,
    ) -> SccStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.scc", G, config)

        return SccStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        consecutive_ids: bool | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.scc", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        consecutive_ids: bool | None = None,
        write_concurrency: int | None = None,
    ) -> SccWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.scc", G, config, write_concurrency, concurrency, write_property
        )

        return SccWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        consecutive_ids: bool | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
        )
        return self._node_property_endpoints.estimate("v2/community.scc.estimate", G, config)
