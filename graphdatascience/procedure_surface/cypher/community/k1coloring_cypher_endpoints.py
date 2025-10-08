from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.k1coloring_endpoints import (
    K1ColoringEndpoints,
    K1ColoringMutateResult,
    K1ColoringStatsResult,
    K1ColoringWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class K1ColoringCypherEndpoints(K1ColoringEndpoints):
    """
    Implementation of the K-1 Coloring algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> K1ColoringMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.k1coloring.mutate", params=params, logging=log_progress
        ).squeeze()

        return K1ColoringMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> K1ColoringStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.k1coloring.stats", params=params, logging=log_progress
        ).squeeze()

        return K1ColoringStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        min_community_size: Optional[int] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.k1coloring.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        min_community_size: Optional[int] = None,
    ) -> K1ColoringWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            batch_size=batch_size,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.k1coloring.write", params=params, logging=log_progress
        ).squeeze()

        return K1ColoringWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            batch_size=batch_size,
            max_iterations=max_iterations,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )
        return estimate_algorithm(
            endpoint="gds.k1coloring.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
