from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.closeness_endpoints import (
    ClosenessEndpoints,
    ClosenessMutateResult,
    ClosenessStatsResult,
    ClosenessWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class ClosenessArrowEndpoints(ClosenessEndpoints):
    """Arrow-based implementation of Closeness Centrality algorithm endpoints."""

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
        use_wasserman_faust: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.closeness", G, config, mutate_property)

        return ClosenessMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        use_wasserman_faust: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.closeness", G, config)

        return ClosenessStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        use_wasserman_faust: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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
        G: GraphV2,
        write_property: str,
        use_wasserman_faust: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: Any | None = None,
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
            "v2/centrality.closeness", G, config, write_concurrency, concurrency, write_property
        )

        return ClosenessWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        use_wasserman_faust: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        algo_config = self._node_property_endpoints.create_estimate_config(
            use_wasserman_faust=use_wasserman_faust,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )

        return self._node_property_endpoints.estimate("v2/centrality.closeness.estimate", G, algo_config)
