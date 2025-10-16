from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import (
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)
from graphdatascience.procedure_surface.api.similarity.knn_filtered_endpoints import KnnFilteredEndpoints
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import rename_similarity_stream_result


class KnnFilteredArrowEndpoints(KnnFilteredEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
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
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnMutateResult:
        config = self._endpoints_helper.create_base_config(
            G,
            nodeProperties=node_properties,
            sourceNodeFilter=source_node_filter,
            node_labels=node_labels,
            relationship_types=relationship_types,
            targetNodeFilter=target_node_filter,
            seedTargetNodes=seed_target_nodes,
            similarityCutoff=similarity_cutoff,
            perturbationRate=perturbation_rate,
            deltaThreshold=delta_threshold,
            sampleRate=sample_rate,
            randomJoins=random_joins,
            initialSampler=initial_sampler,
            maxIterations=max_iterations,
            topK=top_k,
            randomSeed=random_seed,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            sudo=sudo,
            username=username,
        )

        result = self._endpoints_helper.run_job_and_mutate(
            "v2/similarity.knn.filtered", G, config, mutate_property, mutate_relationship_type
        )

        return KnnMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            relationship_types=relationship_types,
            node_labels=node_labels,
            nodeProperties=node_properties,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            seedTargetNodes=seed_target_nodes,
            similarityCutoff=similarity_cutoff,
            perturbationRate=perturbation_rate,
            deltaThreshold=delta_threshold,
            sampleRate=sample_rate,
            randomJoins=random_joins,
            initialSampler=initial_sampler,
            maxIterations=max_iterations,
            topK=top_k,
            randomSeed=random_seed,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            sudo=sudo,
            username=username,
        )

        result = self._endpoints_helper.run_job_and_get_summary("v2/similarity.knn.filtered", G, config)
        result["similarityPairs"] = result.pop("relationshipsWritten", 0)
        return KnnStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._endpoints_helper.create_base_config(
            G,
            nodeProperties=node_properties,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            seedTargetNodes=seed_target_nodes,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            similarityCutoff=similarity_cutoff,
            perturbationRate=perturbation_rate,
            deltaThreshold=delta_threshold,
            sampleRate=sample_rate,
            randomJoins=random_joins,
            initialSampler=initial_sampler,
            maxIterations=max_iterations,
            topK=top_k,
            randomSeed=random_seed,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            sudo=sudo,
            username=username,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/similarity.knn.filtered", G, config)
        rename_similarity_stream_result(result)

        return result

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnWriteResult:
        config = self._endpoints_helper.create_base_config(
            G,
            nodeProperties=node_properties,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            seedTargetNodes=seed_target_nodes,
            nodeLabels=node_labels,
            relationshipTypes=relationship_types,
            similarityCutoff=similarity_cutoff,
            perturbationRate=perturbation_rate,
            deltaThreshold=delta_threshold,
            sampleRate=sample_rate,
            randomJoins=random_joins,
            initialSampler=initial_sampler,
            maxIterations=max_iterations,
            topK=top_k,
            randomSeed=random_seed,
            writeConcurrency=write_concurrency,
            concurrency=concurrency,
            jobId=job_id,
            logProgress=log_progress,
            sudo=sudo,
            username=username,
        )

        result = self._endpoints_helper.run_job_and_write(
            "v2/similarity.knn.filtered",
            G,
            config,
            property_overwrites=write_property,
            relationship_type_overwrite=write_relationship_type,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
        )

        return KnnWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            relationship_types=relationship_types,
            node_labels=node_labels,
            nodeProperties=node_properties,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            seedTargetNodes=seed_target_nodes,
            similarityCutoff=similarity_cutoff,
            perturbationRate=perturbation_rate,
            deltaThreshold=delta_threshold,
            sampleRate=sample_rate,
            randomJoins=random_joins,
            initialSampler=initial_sampler,
            maxIterations=max_iterations,
            topK=top_k,
            randomSeed=random_seed,
            concurrency=concurrency,
            sudo=sudo,
            username=username,
        )

        return self._endpoints_helper.estimate("v2/similarity.knn.filtered.estimate", G, config)
