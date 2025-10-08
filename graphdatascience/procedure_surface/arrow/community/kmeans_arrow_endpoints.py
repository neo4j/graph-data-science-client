from typing import Any, List, Optional, Union

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
        write_back_client: Optional[RemoteWriteBackClient] = None,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = None,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = None,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = None,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = None,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
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
        G: Union[GraphV2, dict[str, Any]],
        node_property: str,
        *,
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[Any] = 4,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        k: Optional[int] = 10,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
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
