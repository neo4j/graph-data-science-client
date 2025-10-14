from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import (
    KnnEndpoints,
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper


class KnnArrowEndpoints(KnnEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = RelationshipEndpointsHelper(
            arrow_client, write_back_client=write_back_client, show_progress=show_progress
        )

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
        config = self._endpoints_helper.create_base_config(
            G,
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

        result = self._endpoints_helper.run_job_and_mutate(
            "v2/similarity.knn", G, config, mutate_property, mutate_relationship_type
        )

        return KnnMutateResult(**result)

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
        config = self._endpoints_helper.create_base_config(
            G,
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

        result = self._endpoints_helper.run_job_and_get_summary("v2/similarity.knn", G, config)
        result["similarityPairs"] = result.pop("relationshipsWritten", 0)
        return KnnStatsResult(**result)

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
        # config = self._endpoints_helper.create_base_config(
        #     G,
        #     nodeProperties=node_properties,
        #     topK=top_k,
        #     similarityCutoff=similarity_cutoff,
        #     deltaThreshold=delta_threshold,
        #     maxIterations=max_iterations,
        #     sampleRate=sample_rate,
        #     perturbationRate=perturbation_rate,
        #     randomJoins=random_joins,
        #     randomSeed=random_seed,
        #     initialSampler=initial_sampler,
        #     relationshipTypes=relationship_types,
        #     nodeLabels=node_labels,
        #     sudo=sudo,
        #     logProgress=log_progress,
        #     username=username,
        #     concurrency=concurrency,
        #     jobId=job_id,
        # )
        # return self._endpoints_helper.run_job_and_stream("v2/similarity.knn", G, config)

        raise NotImplementedError()

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
        config = self._endpoints_helper.create_base_config(
            G,
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

        result = self._endpoints_helper.run_job_and_write(
            "v2/similarity.knn",
            G,
            config,
            relationship_type_overwrite=write_relationship_type,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=None,
        )

        return KnnWriteResult(**result)

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
        config = self._endpoints_helper.create_estimate_config(
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

        return self._endpoints_helper.estimate("v2/similarity.knn.estimate", G, config)
