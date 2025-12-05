from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import KnnEndpoints
from graphdatascience.procedure_surface.api.similarity.knn_filtered_endpoints import KnnFilteredEndpoints
from graphdatascience.procedure_surface.api.similarity.knn_results import (
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.cypher.similarity.knn_filtered_cypher_endpoints import (
    KnnFilteredCypherEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class KnnCypherEndpoints(KnnEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    @property
    def filtered(self) -> KnnFilteredEndpoints:
        return KnnFilteredCypherEndpoints(self._query_runner)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateRelationshipType=mutate_relationship_type,
            mutateProperty=mutate_property,
            nodeProperties=node_properties,
            topK=top_k,
            similarityCutoff=similarity_cutoff,
            deltaThreshold=delta_threshold,
            maxIterations=max_iterations,
            sampleRate=sample_rate,
            perturbationRate=perturbation_rate,
            randomJoins=random_joins,
            randomSeed=random_seed,
            initialSampler=initial_sampler,
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

        result = self._query_runner.call_procedure("gds.knn.mutate", params=params).iloc[0]

        return KnnMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            topK=top_k,
            similarityCutoff=similarity_cutoff,
            deltaThreshold=delta_threshold,
            maxIterations=max_iterations,
            sampleRate=sample_rate,
            perturbationRate=perturbation_rate,
            randomJoins=random_joins,
            randomSeed=random_seed,
            initialSampler=initial_sampler,
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

        result = self._query_runner.call_procedure("gds.knn.stats", params=params, logging=log_progress).iloc[0]

        return KnnStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            topK=top_k,
            similarityCutoff=similarity_cutoff,
            deltaThreshold=delta_threshold,
            maxIterations=max_iterations,
            sampleRate=sample_rate,
            perturbationRate=perturbation_rate,
            randomJoins=random_joins,
            randomSeed=random_seed,
            initialSampler=initial_sampler,
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

        result = self._query_runner.call_procedure("gds.knn.stream", params=params, logging=log_progress)

        return result

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> KnnWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeRelationshipType=write_relationship_type,
            writeProperty=write_property,
            nodeProperties=node_properties,
            topK=top_k,
            similarityCutoff=similarity_cutoff,
            deltaThreshold=delta_threshold,
            maxIterations=max_iterations,
            sampleRate=sample_rate,
            perturbationRate=perturbation_rate,
            randomJoins=random_joins,
            randomSeed=random_seed,
            initialSampler=initial_sampler,
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

        result = self._query_runner.call_procedure("gds.knn.write", params=params, logging=log_progress).iloc[0]

        return KnnWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            topK=top_k,
            similarityCutoff=similarity_cutoff,
            deltaThreshold=delta_threshold,
            maxIterations=max_iterations,
            sampleRate=sample_rate,
            perturbationRate=perturbation_rate,
            randomJoins=random_joins,
            randomSeed=random_seed,
            initialSampler=initial_sampler,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        return estimate_algorithm(
            endpoint="gds.knn.stats.estimate", query_runner=self._query_runner, G=G, algo_config=config
        )
