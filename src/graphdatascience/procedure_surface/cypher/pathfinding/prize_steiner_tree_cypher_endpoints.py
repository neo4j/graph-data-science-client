from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.prize_steiner_tree_endpoints import (
    PrizeSteinerTreeEndpoints,
    PrizeSteinerTreeMutateResult,
    PrizeSteinerTreeStatsResult,
    PrizeSteinerTreeWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class PrizeSteinerTreeCypherEndpoints(PrizeSteinerTreeEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(
        self,
        G: GraphV2,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            prizeProperty=prize_property,
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
            "gds.prizeSteinerTree.stream",
            params=params,
            logging=log_progress,
        )

    def stats(
        self,
        G: GraphV2,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> PrizeSteinerTreeStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            prizeProperty=prize_property,
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
            "gds.prizeSteinerTree.stats", params=params, logging=log_progress
        ).squeeze()

        return PrizeSteinerTreeStatsResult(**result)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> PrizeSteinerTreeMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateRelationshipType=mutate_relationship_type,
            mutateProperty=mutate_property,
            prizeProperty=prize_property,
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
            "gds.prizeSteinerTree.mutate", params=params, logging=log_progress
        ).squeeze()

        return PrizeSteinerTreeMutateResult(**result)

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> PrizeSteinerTreeWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeRelationshipType=write_relationship_type,
            writeProperty=write_property,
            prizeProperty=prize_property,
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
            "gds.prizeSteinerTree.write", params=params, logging=log_progress
        ).squeeze()

        return PrizeSteinerTreeWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            prizeProperty=prize_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return estimate_algorithm("gds.prizeSteinerTree.stats.estimate", self._query_runner, G, config)
