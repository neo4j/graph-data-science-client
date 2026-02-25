from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.max_flow_min_cost_endpoints import (
    MaxFlowMinCostEndpoints,
    MaxFlowMinCostMutateResult,
    MaxFlowMinCostStatsResult,
    MaxFlowMinCostWriteResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import (
    map_max_flow_stream_result,
)


class MaxFlowMinCostArrowEndpoints(MaxFlowMinCostEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._relationship_endpoints = RelationshipEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        mutate_property: str,
        mutate_relationship_type: str,
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> MaxFlowMinCostMutateResult:
        config = self._relationship_endpoints.create_base_config(
            G,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            costProperty=cost_property,
            alpha=alpha,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes,
            sudo=sudo,
            targetNodes=target_nodes,
            username=username,
        )

        result = self._relationship_endpoints.run_job_and_mutate(
            "v2/pathfinding.maxFlow.minCost",
            config,
            mutate_property,
            mutate_relationship_type,
        )

        return MaxFlowMinCostMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> MaxFlowMinCostStatsResult:
        config = self._relationship_endpoints.create_base_config(
            G,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            costProperty=cost_property,
            alpha=alpha,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes,
            sudo=sudo,
            targetNodes=target_nodes,
            username=username,
        )

        computation_result = self._relationship_endpoints.run_job_and_get_summary(
            "v2/pathfinding.maxFlow.minCost", config
        )

        return MaxFlowMinCostStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = self._relationship_endpoints.create_base_config(
            G,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            costProperty=cost_property,
            alpha=alpha,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes,
            sudo=sudo,
            targetNodes=target_nodes,
            username=username,
        )

        result = self._relationship_endpoints.run_job_and_stream("v2/pathfinding.maxFlow.minCost", G, config)
        map_max_flow_stream_result(result)
        return result

    def write(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        write_property: str,
        write_relationship_type: str,
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> MaxFlowMinCostWriteResult:
        config = self._relationship_endpoints.create_base_config(
            G,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            costProperty=cost_property,
            alpha=alpha,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes,
            sudo=sudo,
            targetNodes=target_nodes,
            username=username,
        )

        result = self._relationship_endpoints.run_job_and_write(
            "v2/pathfinding.maxFlow.minCost",
            G,
            config,
            relationship_type_overwrite=write_relationship_type,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return MaxFlowMinCostWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        config = self._relationship_endpoints.create_estimate_config(
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            costProperty=cost_property,
            alpha=alpha,
            concurrency=concurrency,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes,
            targetNodes=target_nodes,
        )

        return self._relationship_endpoints.estimate("v2/pathfinding.maxFlow.minCost.estimate", G, config)
