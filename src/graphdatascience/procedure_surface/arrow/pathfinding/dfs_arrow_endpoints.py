from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.dfs_endpoints import (
    DFSEndpoints,
    DFSMutateResult,
    DFSStatsResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import aggregate_traversal_rels


class DFSArrowEndpoints(DFSEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = RelationshipEndpointsHelper(
            arrow_client, write_back_client=write_back_client, show_progress=show_progress
        )

    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: int | list[int] | None = None,
        max_depth: int = -1,
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
            jobId=job_id,
            logProgress=log_progress,
            maxDepth=max_depth,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNode=source_node,
            sudo=sudo,
            targetNodes=target_nodes,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/pathfinding.dfs", G, config)

        return aggregate_traversal_rels(result, source_node)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        source_node: int,
        target_nodes: int | list[int] | None = None,
        max_depth: int = -1,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DFSMutateResult:
        config = self._endpoints_helper.create_base_config(
            G,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            maxDepth=max_depth,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNode=source_node,
            sudo=sudo,
            targetNodes=target_nodes,
        )

        result = self._endpoints_helper.run_job_and_mutate(
            "v2/pathfinding.dfs",
            config,
            mutate_property="index",
            mutate_relationship_type=mutate_relationship_type,
        )

        return DFSMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: int | list[int] | None = None,
        max_depth: int = -1,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DFSStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            maxDepth=max_depth,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNode=source_node,
            sudo=sudo,
            targetNodes=target_nodes,
        )

        computation_result = self._endpoints_helper.run_job_and_get_summary("v2/pathfinding.dfs", config)

        return DFSStatsResult(**computation_result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_nodes: int | list[int] | None = None,
        max_depth: int = -1,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            concurrency=concurrency,
            maxDepth=max_depth,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNode=source_node,
            targetNodes=target_nodes,
        )

        return self._endpoints_helper.estimate("v2/pathfinding.dfs.estimate", G, config)
