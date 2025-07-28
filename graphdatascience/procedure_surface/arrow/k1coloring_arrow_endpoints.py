from typing import Any, List, Optional

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.estimation_result import EstimationResult
from ..api.k1coloring_endpoints import (
    K1ColoringEndpoints,
    K1ColoringMutateResult,
    K1ColoringStatsResult,
    K1ColoringWriteResult,
)
from .node_property_endpoints import NodePropertyEndpoints


class K1ColoringArrowEndpoints(K1ColoringEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> K1ColoringMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result, node_properties_written = self._node_property_endpoints.run_job_and_mutate(
            "v2/community.k1coloring", G, config, mutate_property
        )

        computation_result["nodeCount"] = node_properties_written
        computation_result["mutateMillis"] = 0

        return K1ColoringMutateResult(**computation_result)

    def stats(
        self,
        G: Graph,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> K1ColoringStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.k1coloring", G, config)

        return K1ColoringStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        min_community_size: Optional[int] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.k1coloring", G, config)

    def write(
        self,
        G: Graph,
        write_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
        min_community_size: Optional[int] = None,
    ) -> K1ColoringWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result, write_millis = self._node_property_endpoints.run_job_and_write(
            "v2/community.k1coloring", G, config, write_concurrency, concurrency
        )

        computation_result["writeMillis"] = write_millis

        return K1ColoringWriteResult(**computation_result)

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        return self._node_property_endpoints.estimate("v2/community.k1coloring.estimate", G, projection_config)
