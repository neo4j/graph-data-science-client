from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.bfs_endpoints import (
    BFSEndpoints,
    BFSMutateResult,
    BFSStatsResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class BFSCypherEndpoints(BFSEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_depth=max_depth,
            node_labels=node_labels,
            relationship_types=relationship_types,
            source_node=source_node,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.bfs.stream", params=params, logging=log_progress)

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
    ) -> BFSMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_relationship_type=mutate_relationship_type,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_depth=max_depth,
            node_labels=node_labels,
            relationship_types=relationship_types,
            source_node=source_node,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.bfs.mutate", params=params, logging=log_progress
        ).squeeze()

        return BFSMutateResult(**result.to_dict())

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
    ) -> BFSStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_depth=max_depth,
            node_labels=node_labels,
            relationship_types=relationship_types,
            source_node=source_node,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.bfs.stats", params=params, logging=log_progress
        ).squeeze()

        return BFSStatsResult(**result.to_dict())

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
        algo_config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            max_depth=max_depth,
            node_labels=node_labels,
            relationship_types=relationship_types,
            source_node=source_node,
            target_nodes=target_nodes,
        )

        return estimate_algorithm(
            endpoint="gds.bfs.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
