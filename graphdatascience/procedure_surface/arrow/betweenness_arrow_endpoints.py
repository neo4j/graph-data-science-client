from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.betweenness_endpoints import (
    BetweennessEndpoints,
    BetweennessMutateResult,
    BetweennessStatsResult,
    BetweennessWriteResult,
)
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class BetweennessArrowEndpoints(BetweennessEndpoints):
    """Arrow-based implementation of Betweenness Centrality algorithm endpoints."""

    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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
        G: Graph,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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
        G: Graph,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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
        G: Graph,
        write_property: str,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
        write_to_result_store: Optional[bool] = None,
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
            write_to_result_store=write_to_result_store,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.betweenness", G, config, write_concurrency, concurrency
        )

        return BetweennessWriteResult(**result)

    def estimate(self, G: Union[Graph, dict[str, Any]]) -> EstimationResult:
        return self._node_property_endpoints.estimate("v2/centrality.betweenness.estimate", G)
