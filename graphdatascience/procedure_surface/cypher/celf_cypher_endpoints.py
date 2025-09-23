from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.celf_endpoints import (
    CelfEndpoints,
    CelfMutateResult,
    CelfStatsResult,
    CelfWriteResult,
)

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter
from .estimation_utils import estimate_algorithm


class CelfCypherEndpoints(CelfEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        seed_set_size: int,
        mutate_property: str,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> CelfMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            seedSetSize=seed_set_size,
            mutateProperty=mutate_property,
            propagationProbability=propagation_probability,
            monteCarloSimulations=monte_carlo_simulations,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.influenceMaximization.celf.mutate", params=params, logging=log_progress
        ).squeeze()
        return CelfMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> CelfStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            seedSetSize=seed_set_size,
            propagationProbability=propagation_probability,
            monteCarloSimulations=monte_carlo_simulations,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.influenceMaximization.celf.stats", params=params, logging=log_progress
        ).squeeze()
        return CelfStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            seedSetSize=seed_set_size,
            propagationProbability=propagation_probability,
            monteCarloSimulations=monte_carlo_simulations,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.influenceMaximization.celf.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        seed_set_size: int,
        write_property: str,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> CelfWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            seedSetSize=seed_set_size,
            writeProperty=write_property,
            propagationProbability=propagation_probability,
            monteCarloSimulations=monte_carlo_simulations,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.influenceMaximization.celf.write", params=params, logging=log_progress
        ).squeeze()
        return CelfWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            seedSetSize=seed_set_size,
            propagationProbability=propagation_probability,
            monteCarloSimulations=monte_carlo_simulations,
            randomSeed=random_seed,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            concurrency=concurrency,
        )

        return estimate_algorithm("gds.influenceMaximization.celf.stats.estimate", self._query_runner, G, algo_config)
