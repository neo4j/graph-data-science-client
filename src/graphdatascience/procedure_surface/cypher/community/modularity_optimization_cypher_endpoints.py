from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationEndpoints,
    ModularityOptimizationMutateResult,
    ModularityOptimizationStatsResult,
    ModularityOptimizationWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class ModularityOptimizationCypherEndpoints(ModularityOptimizationEndpoints):
    """
    Implementation of the Modularity Optimization algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> ModularityOptimizationMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.modularityOptimization.mutate", params=params
        ).squeeze()

        return ModularityOptimizationMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> ModularityOptimizationStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.modularityOptimization.stats", params=params).squeeze()

        return ModularityOptimizationStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.modularityOptimization.stream", params=params)

        return result

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> ModularityOptimizationWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.modularityOptimization.write", params=params).squeeze()

        return ModularityOptimizationWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        tolerance: float = 0.0001,
    ) -> EstimationResult:
        from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

        config = ConfigConverter.convert_to_gds_config(
            batch_size=batch_size,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            tolerance=tolerance,
        )

        return estimate_algorithm("gds.modularityOptimization.stream.estimate", self._query_runner, G, config)
