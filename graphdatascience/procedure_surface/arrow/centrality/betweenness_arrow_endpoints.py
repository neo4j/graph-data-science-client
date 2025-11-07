from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.betweenness_endpoints import (
    BetweennessEndpoints,
    BetweennessMutateResult,
    BetweennessStatsResult,
    BetweennessWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class BetweennessArrowEndpoints(BetweennessEndpoints):
    """Arrow-based implementation of Betweenness Centrality algorithm endpoints."""

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> BetweennessMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.betweenness", config, mutate_property)

        return BetweennessMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> BetweennessStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.betweenness", config)

        return BetweennessStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.betweenness", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        write_concurrency: Any | None = None,
    ) -> BetweennessWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.betweenness",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return BetweennessWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
        )
        return self._node_property_endpoints.estimate("v2/centrality.betweenness.estimate", G, config)
