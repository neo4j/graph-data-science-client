from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.celf_endpoints import (
    CelfEndpoints,
    CelfMutateResult,
    CelfStatsResult,
    CelfWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class CelfCypherEndpoints(CelfEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        seed_set_size: int,
        mutate_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        monte_carlo_simulations: int = 100,
        propagation_probability: float = 0.1,
        random_seed: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        monte_carlo_simulations: int = 100,
        propagation_probability: float = 0.1,
        random_seed: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        monte_carlo_simulations: int = 100,
        propagation_probability: float = 0.1,
        random_seed: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
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
        propagation_probability: float = 0.1,
        monte_carlo_simulations: int = 100,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
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
        G: GraphV2 | dict[str, Any],
        seed_set_size: int,
        propagation_probability: float | None = None,
        monte_carlo_simulations: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
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
