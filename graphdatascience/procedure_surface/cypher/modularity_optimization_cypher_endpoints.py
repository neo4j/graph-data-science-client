from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationEndpoints,
    ModularityOptimizationMutateResult,
    ModularityOptimizationStatsResult,
    ModularityOptimizationWriteResult,
)
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
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
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
            configuration=config,
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
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
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
            configuration=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.modularityOptimization.stats", params=params).squeeze()

        return ModularityOptimizationStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
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
            configuration=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.modularityOptimization.stream", params=params)

        return result

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
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
            write_to_result_store=write_to_result_store,
        )

        params = CallParameters(
            graph_name=G.name(),
            configuration=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.modularityOptimization.write", params=params).squeeze()

        return ModularityOptimizationWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        tolerance: Optional[float] = None,
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
