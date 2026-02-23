from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.max_flow_min_cost_endpoints import (
    MaxFlowMinCostEndpoints,
    MaxFlowMinCostMutateResult,
    MaxFlowMinCostStatsResult,
    MaxFlowMinCostWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class MaxFlowMinCostCypherEndpoints(MaxFlowMinCostEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            mutateRelationshipType=mutate_relationship_type,
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.maxFlow.minCost.mutate", params=params, logging=log_progress
        ).squeeze()

        return MaxFlowMinCostMutateResult(**cypher_result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.maxFlow.minCost.stats", params=params, logging=log_progress
        ).squeeze()

        return MaxFlowMinCostStatsResult(**cypher_result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.maxFlow.minCost.stream", params=params, logging=log_progress
        )

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
        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            writeRelationshipType=write_relationship_type,
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
            writeConcurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.maxFlow.minCost.write", params=params, logging=log_progress
        ).squeeze()

        return MaxFlowMinCostWriteResult(**result.to_dict())

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
        algo_config = ConfigConverter.convert_to_gds_config(
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            costProperty=cost_property,
            alpha=alpha,
            concurrency=concurrency,
            sourceNodes=source_nodes,
            targetNodes=target_nodes,
        )
        return estimate_algorithm(
            endpoint="gds.maxFlow.minCost.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
