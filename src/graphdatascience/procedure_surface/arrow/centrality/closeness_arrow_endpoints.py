from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.closeness_endpoints import (
    ClosenessEndpoints,
    ClosenessMutateResult,
    ClosenessStatsResult,
    ClosenessWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class ClosenessArrowEndpoints(ClosenessEndpoints):
    """Arrow-based implementation of Closeness Centrality algorithm endpoints."""

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: Graph,
        *,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        config = self._node_property_endpoints.create_base_config(
            G,
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )
        return self._node_property_endpoints.run_job(G, "v2/centrality.closeness", config)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ClosenessMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.closeness", config, mutate_property)

        return ClosenessMutateResult(**result)

    def stats(
        self,
        G: Graph,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ClosenessStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.closeness", config)

        return ClosenessStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.closeness", G, config)

    def write(
        self,
        G: Graph,
        write_property: str,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> ClosenessWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.closeness",
            G,
            config,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
            property_overwrites=write_property,
        )

        return ClosenessWriteResult(**result)

    def estimate(
        self,
        G: Graph | dict[str, Any],
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        algo_config = self._node_property_endpoints.create_estimate_config(
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )

        return self._node_property_endpoints.estimate("v2/centrality.closeness.estimate", G, algo_config)
