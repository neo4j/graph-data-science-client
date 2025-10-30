from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_endpoints import (
    AStarMutateResult,
    AStarWriteResult,
    SourceTargetAStarEndpoints,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class AStarCypherEndpoints(SourceTargetAStarEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            sourceNode=source_node,
            targetNode=target_node,
            latitudeProperty=latitude_property,
            longitudeProperty=longitude_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            "gds.shortestPath.astar.stream",
            params=params,
            logging=log_progress,
            yields=["sourceNode", "targetNode", "totalCost", "nodeIds", "costs", "index"],  # skip path column
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> AStarMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateRelationshipType=mutate_relationship_type,
            sourceNode=source_node,
            targetNode=target_node,
            latitudeProperty=latitude_property,
            longitudeProperty=longitude_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            "gds.shortestPath.astar.mutate", params=params, logging=log_progress
        ).iloc[0]

        return AStarMutateResult(**result.to_dict())

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        write_node_ids: bool = False,
        write_costs: bool = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> AStarWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeRelationshipType=write_relationship_type,
            sourceNode=source_node,
            targetNode=target_node,
            latitudeProperty=latitude_property,
            longitudeProperty=longitude_property,
            writeNodeIds=write_node_ids,
            writeCosts=write_costs,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            "gds.shortestPath.astar.write", params=params, logging=log_progress
        ).iloc[0]

        return AStarWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            sourceNode=source_node,
            targetNode=target_node,
            latitudeProperty=latitude_property,
            longitudeProperty=longitude_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return estimate_algorithm("gds.shortestPath.astar.stream.estimate", self._query_runner, G, config)
