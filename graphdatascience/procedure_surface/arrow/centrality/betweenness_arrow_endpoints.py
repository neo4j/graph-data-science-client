from typing import Any, List, Optional, Union

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
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class BetweennessArrowEndpoints(BetweennessEndpoints):
    """Arrow-based implementation of Betweenness Centrality algorithm endpoints."""

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
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
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

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/centrality.betweenness", G, config, mutate_property
        )

        return BetweennessMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/centrality.betweenness", G, config
        )

        return BetweennessStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
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
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
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
            "v2/centrality.betweenness", G, config, write_concurrency, concurrency, write_property
        )

        return BetweennessWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
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
