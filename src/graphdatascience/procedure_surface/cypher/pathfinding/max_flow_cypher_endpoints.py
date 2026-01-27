from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.max_flow_endpoints import (
    MaxFlowEndpoints,
    MaxFlowMutateResult,
    MaxFlowStatsResult,
    MaxFlowWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class MaxFlowCypherEndpoints(MaxFlowEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        source_nodes: list[int],
        mutate_property: str,
        mutate_relationship_type: str,
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        target_nodes: list[int] | None = None,
        username: str | None = None,
    ) -> MaxFlowMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            mutateRelationshipType=mutate_relationship_type,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes or [],
            sudo=sudo,
            targetNodes=target_nodes or [],
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.maxFlow.mutate", params=params, logging=log_progress
        ).squeeze()

        return MaxFlowMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        source_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        target_nodes: list[int] | None = None,
        username: str | None = None,
    ) -> MaxFlowStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes or [],
            sudo=sudo,
            targetNodes=target_nodes or [],
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.maxFlow.stats", params=params, logging=log_progress
        ).squeeze()

        raw_result = cypher_result.to_dict()
        # return field got added in 2.24
        if "postProcessingMillis" not in raw_result:
            raw_result["postProcessingMillis"] = 0

        return MaxFlowStatsResult(**raw_result)

    def stream(
        self,
        G: GraphV2,
        source_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        target_nodes: list[int] | None = None,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes or [],
            sudo=sudo,
            targetNodes=target_nodes or [],
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.maxFlow.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        source_nodes: list[int],
        write_property: str,
        write_relationship_type: str,
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        target_nodes: list[int] | None = None,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> MaxFlowWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            writeRelationshipType=write_relationship_type,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            sourceNodes=source_nodes or [],
            sudo=sudo,
            targetNodes=target_nodes or [],
            username=username,
            writeConcurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.maxFlow.write", params=params, logging=log_progress
        ).squeeze()

        return MaxFlowWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        target_nodes: list[int] | None = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            capacityProperty=capacity_property,
            nodeCapacityProperty=node_capacity_property,
            concurrency=concurrency,
            sourceNodes=source_nodes or [],
            targetNodes=target_nodes or [],
        )
        return estimate_algorithm(
            endpoint="gds.maxFlow.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
