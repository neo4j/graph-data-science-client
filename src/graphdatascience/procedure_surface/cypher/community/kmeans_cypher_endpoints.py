from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import (
    KMeansEndpoints,
    KMeansMutateResult,
    KMeansStatsResult,
    KMeansWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class KMeansCypherEndpoints(KMeansEndpoints):
    """
    Implementation of the K-Means algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> KMeansMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            node_property=node_property,
            mutate_property=mutate_property,
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.kmeans.mutate", params=params, logging=log_progress
        ).squeeze()

        return KMeansMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> KMeansStatsResult:
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.kmeans.stats", params=params, logging=log_progress
        ).squeeze()

        return KMeansStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.kmeans.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        node_property: str,
        write_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> KMeansWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            node_property=node_property,
            write_property=write_property,
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
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.kmeans.write", params=params, logging=log_progress
        ).squeeze()  # type: ignore

        return KMeansWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        k: int = 10,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
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
        return estimate_algorithm(
            endpoint="gds.kmeans.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
