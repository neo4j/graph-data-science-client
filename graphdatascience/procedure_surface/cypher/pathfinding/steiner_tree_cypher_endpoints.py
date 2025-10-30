from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.steiner_tree_endpoints import (
    SteinerTreeEndpoints,
    SteinerTreeMutateResult,
    SteinerTreeStatsResult,
    SteinerTreeWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class SteinerTreeCypherEndpoints(SteinerTreeEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
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
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
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
            "gds.steinerTree.stream",
            params=params,
            logging=log_progress,
        )

    def stats(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> SteinerTreeStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
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
            "gds.steinerTree.stats", params=params, logging=log_progress
        ).squeeze()

        return SteinerTreeStatsResult(**result)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> SteinerTreeMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateRelationshipType=mutate_relationship_type,
            mutateProperty=mutate_property,
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
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
            "gds.steinerTree.mutate", params=params, logging=log_progress
        ).squeeze()

        return SteinerTreeMutateResult(**result)

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> SteinerTreeWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeRelationshipType=write_relationship_type,
            writeProperty=write_property,
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
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
            "gds.steinerTree.write", params=params, logging=log_progress
        ).squeeze()

        return SteinerTreeWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return estimate_algorithm("gds.steinerTree.stats.estimate", self._query_runner, G, config)
