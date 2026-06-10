from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.random_walk_endpoints import (
    RandomWalkEndpoints,
    RandomWalkMutateResult,
    RandomWalkStatsResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class RandomWalkCypherEndpoints(RandomWalkEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            in_out_factor=in_out_factor,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            return_factor=return_factor,
            source_nodes=source_nodes,
            sudo=sudo,
            username=username,
            walk_buffer_size=walk_buffer_size,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.randomWalk.stream", params=params, logging=log_progress)

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
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            in_out_factor=in_out_factor,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            return_factor=return_factor,
            source_nodes=source_nodes,
            sudo=sudo,
            username=username,
            walk_buffer_size=walk_buffer_size,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.randomWalk.mutate", params=params, logging=log_progress
        ).squeeze()

        return RandomWalkMutateResult(**result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            in_out_factor=in_out_factor,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            return_factor=return_factor,
            source_nodes=source_nodes,
            sudo=sudo,
            username=username,
            walk_buffer_size=walk_buffer_size,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.randomWalk.stats", params=params, logging=log_progress
        ).squeeze()

        return RandomWalkStatsResult(**result.to_dict())

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
        algo_config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            in_out_factor=in_out_factor,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            return_factor=return_factor,
            source_nodes=source_nodes,
            walk_buffer_size=walk_buffer_size,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
        )

        return estimate_algorithm(
            endpoint="gds.randomWalk.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
