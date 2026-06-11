from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.random_walk_endpoints import (
    RandomWalkEndpoints,
    RandomWalkMutateResult,
    RandomWalkStatsResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class RandomWalkArrowEndpoints(RandomWalkEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = NodePropertyEndpointsHelper(
            arrow_client, write_protocol=write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: Graph,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        config = self._endpoints_helper.create_base_config(
            G,
            concurrency=concurrency,
            inOutFactor=in_out_factor,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            relationshipWeightProperty=relationship_weight_property,
            returnFactor=return_factor,
            sourceNodes=source_nodes,
            sudo=sudo,
            username=username,
            walkBufferSize=walk_buffer_size,
            walkLength=walk_length,
            walksPerNode=walks_per_node,
        )
        return self._endpoints_helper.run_job(G, "v2/pathfinding.randomWalk", config)

    def stream(
        self,
        G: Graph,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._endpoints_helper.create_base_config(
            G,
            concurrency=concurrency,
            inOutFactor=in_out_factor,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            relationshipWeightProperty=relationship_weight_property,
            returnFactor=return_factor,
            sourceNodes=source_nodes,
            sudo=sudo,
            username=username,
            walkBufferSize=walk_buffer_size,
            walkLength=walk_length,
            walksPerNode=walks_per_node,
        )

        return self._endpoints_helper.run_job_and_stream("v2/pathfinding.randomWalk", G, config)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> RandomWalkMutateResult:
        raise Exception("Mutation is not supported for RandomWalk")

    def stats(
        self,
        G: Graph,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> RandomWalkStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            concurrency=concurrency,
            inOutFactor=in_out_factor,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            relationshipWeightProperty=relationship_weight_property,
            returnFactor=return_factor,
            sourceNodes=source_nodes,
            sudo=sudo,
            username=username,
            walkBufferSize=walk_buffer_size,
            walkLength=walk_length,
            walksPerNode=walks_per_node,
        )

        result = self._endpoints_helper.run_job_and_get_summary("v2/pathfinding.randomWalk", config)

        return RandomWalkStatsResult(**result)

    def estimate(
        self,
        G: Graph | dict[str, Any],
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            concurrency=concurrency,
            inOutFactor=in_out_factor,
            nodeLabels=node_labels,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            relationshipWeightProperty=relationship_weight_property,
            returnFactor=return_factor,
            sourceNodes=source_nodes,
            walkBufferSize=walk_buffer_size,
            walkLength=walk_length,
            walksPerNode=walks_per_node,
        )

        return self._endpoints_helper.estimate("v2/pathfinding.randomWalk.estimate", G, config)
