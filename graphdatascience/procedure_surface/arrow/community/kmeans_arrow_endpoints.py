from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import (
    KMeansEndpoints,
    KMeansMutateResult,
    KMeansStatsResult,
    KMeansWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class KMeansArrowEndpoints(KMeansEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        compute_silhouette: bool | None = False,
        concurrency: int | None = None,
        delta_threshold: float | None = 0.05,
        initial_sampler: str | None = "UNIFORM",
        job_id: str | None = None,
        k: int | None = 10,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        number_of_restarts: int | None = 1,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> KMeansMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            compute_silhouette=compute_silhouette,
            concurrency=concurrency,
            delta_threshold=delta_threshold,
            initial_sampler=initial_sampler,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            number_of_restarts=number_of_restarts,
            random_seed=random_seed,
            relationship_types=relationship_types,
            seed_centroids=seed_centroids,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.kmeans", G, config, mutate_property)

        return KMeansMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        node_property: str,
        *,
        compute_silhouette: bool | None = False,
        concurrency: int | None = None,
        delta_threshold: float | None = 0.05,
        initial_sampler: str | None = "UNIFORM",
        job_id: str | None = None,
        k: int | None = 10,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        number_of_restarts: int | None = 1,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> KMeansStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            compute_silhouette=compute_silhouette,
            concurrency=concurrency,
            delta_threshold=delta_threshold,
            initial_sampler=initial_sampler,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            number_of_restarts=number_of_restarts,
            random_seed=random_seed,
            relationship_types=relationship_types,
            seed_centroids=seed_centroids,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.kmeans", G, config)

        return KMeansStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        node_property: str,
        *,
        compute_silhouette: bool | None = False,
        concurrency: int | None = None,
        delta_threshold: float | None = 0.05,
        initial_sampler: str | None = "UNIFORM",
        job_id: str | None = None,
        k: int | None = 10,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        number_of_restarts: int | None = 1,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            compute_silhouette=compute_silhouette,
            concurrency=concurrency,
            delta_threshold=delta_threshold,
            initial_sampler=initial_sampler,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            number_of_restarts=number_of_restarts,
            random_seed=random_seed,
            relationship_types=relationship_types,
            seed_centroids=seed_centroids,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.kmeans", G, config)

    def write(
        self,
        G: GraphV2,
        node_property: str,
        write_property: str,
        *,
        compute_silhouette: bool | None = False,
        concurrency: int | None = None,
        delta_threshold: float | None = 0.05,
        initial_sampler: str | None = "UNIFORM",
        job_id: str | None = None,
        k: int | None = 10,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        number_of_restarts: int | None = 1,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: Any | None = None,
    ) -> KMeansWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            compute_silhouette=compute_silhouette,
            concurrency=concurrency,
            delta_threshold=delta_threshold,
            initial_sampler=initial_sampler,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            number_of_restarts=number_of_restarts,
            random_seed=random_seed,
            relationship_types=relationship_types,
            seed_centroids=seed_centroids,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.kmeans", G, config, write_concurrency, concurrency, write_property
        )

        return KMeansWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_property: str,
        *,
        compute_silhouette: bool | None = False,
        concurrency: Any | None = 4,
        delta_threshold: float | None = 0.05,
        initial_sampler: str | None = "UNIFORM",
        k: int | None = 10,
        max_iterations: int | None = 10,
        node_labels: list[str] | None = None,
        number_of_restarts: int | None = 1,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        seed_centroids: list[list[float]] | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            node_property=node_property,
            compute_silhouette=compute_silhouette,
            concurrency=concurrency,
            delta_threshold=delta_threshold,
            initial_sampler=initial_sampler,
            k=k,
            max_iterations=max_iterations,
            node_labels=node_labels,
            number_of_restarts=number_of_restarts,
            random_seed=random_seed,
            relationship_types=relationship_types,
            seed_centroids=seed_centroids,
        )
        return self._node_property_endpoints.estimate("v2/community.kmeans.estimate", G, config)
