from collections import OrderedDict
from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..api.k1coloring_endpoints import (
    K1ColoringEndpoints,
    K1ColoringMutateResult,
    K1ColoringStatsResult,
    K1ColoringWriteResult,
)
from ..utils.config_converter import ConfigConverter


class K1ColoringCypherEndpoints(K1ColoringEndpoints):
    """
    Implementation of the K-1 Coloring algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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

        cypher_result = self._query_runner.call_procedure(endpoint="gds.k1coloring.mutate", params=params).squeeze()

        return K1ColoringMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: Graph,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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

        cypher_result = self._query_runner.call_procedure(endpoint="gds.k1coloring.stats", params=params).squeeze()

        return K1ColoringStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: Graph,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
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

        return self._query_runner.call_procedure(endpoint="gds.k1coloring.stream", params=params)

    def write(
        self,
        G: Graph,
        write_property: str,
        batch_size: Optional[int] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        write_to_result_store: Optional[bool] = None,
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
        )

        if write_concurrency is not None:
            config["writeConcurrency"] = write_concurrency
        if write_to_result_store is not None:
            config["writeToResultStore"] = write_to_result_store

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.k1coloring.write", params=params).squeeze()

        return K1ColoringWriteResult(**result.to_dict())

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        config: Union[dict[str, Any]] = OrderedDict()

        if G is not None:
            config["graphNameOrConfiguration"] = G.name()
        elif projection_config is not None:
            config["graphNameOrConfiguration"] = projection_config
        else:
            raise ValueError("Either graph_name or projection_config must be provided.")

        config["algoConfig"] = {}

        params = CallParameters(**config)

        result = self._query_runner.call_procedure(endpoint="gds.k1coloring.stats.estimate", params=params).squeeze()

        return EstimationResult(**result.to_dict())
