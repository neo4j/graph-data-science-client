from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import (
    KMeansEndpoints,
    KMeansMutateResult,
    KMeansStatsResult,
    KMeansWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
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
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = False,
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
            write_to_result_store=write_to_result_store,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.kmeans.write", params=params, logging=log_progress
        ).squeeze()  # type: ignore

        return KMeansWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        node_property: str,
        *,
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
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
