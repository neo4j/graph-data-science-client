from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import (
    KnnEndpoints,
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class KnnCypherEndpoints(KnnEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        node_properties: Union[str, List[str], dict[str, str]],
        top_k: Optional[int] = None,
        similarity_cutoff: Optional[float] = None,
        delta_threshold: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_rate: Optional[float] = None,
        perturbation_rate: Optional[float] = None,
        random_joins: Optional[int] = None,
        random_seed: Optional[int] = None,
        initial_sampler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        node_properties: Union[str, List[str], dict[str, str]],
        top_k: Optional[int] = None,
        similarity_cutoff: Optional[float] = None,
        delta_threshold: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_rate: Optional[float] = None,
        perturbation_rate: Optional[float] = None,
        random_joins: Optional[int] = None,
        random_seed: Optional[int] = None,
        initial_sampler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        node_properties: Union[str, List[str], dict[str, str]],
        top_k: Optional[int] = None,
        similarity_cutoff: Optional[float] = None,
        delta_threshold: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_rate: Optional[float] = None,
        perturbation_rate: Optional[float] = None,
        random_joins: Optional[int] = None,
        random_seed: Optional[int] = None,
        initial_sampler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        node_properties: Union[str, List[str], dict[str, str]],
        top_k: Optional[int] = None,
        similarity_cutoff: Optional[float] = None,
        delta_threshold: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_rate: Optional[float] = None,
        perturbation_rate: Optional[float] = None,
        random_joins: Optional[int] = None,
        random_seed: Optional[int] = None,
        initial_sampler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
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
        G: GraphV2,
        node_properties: Union[str, List[str], dict[str, str]],
        top_k: Optional[int] = None,
        similarity_cutoff: Optional[float] = None,
        delta_threshold: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_rate: Optional[float] = None,
        perturbation_rate: Optional[float] = None,
        random_joins: Optional[int] = None,
        random_seed: Optional[int] = None,
        initial_sampler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return estimate_algorithm(
            endpoint="gds.knn.stats.estimate", query_runner=self._query_runner, G=G, algo_config=config
        )
