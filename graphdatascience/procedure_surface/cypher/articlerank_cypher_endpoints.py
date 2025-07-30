from typing import Any, List, Optional

from pandas import DataFrame

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.articlerank_endpoints import (
    ArticleRankEndpoints,
    ArticleRankMutateResult,
    ArticleRankStatsResult,
    ArticleRankWriteResult,
)
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class ArticleRankCypherEndpoints(ArticleRankEndpoints):
    """
    Implementation of the ArticleRank algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> ArticleRankMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.articleRank.mutate", params=params).squeeze()

        return ArticleRankMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: Graph,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> ArticleRankStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.articleRank.stats", params=params).squeeze()

        return ArticleRankStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: Graph,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.articleRank.stream", params=params)

    def write(
        self,
        G: Graph,
        write_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
    ) -> ArticleRankWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
            write_concurrency=write_concurrency,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.articleRank.write", params=params).squeeze()

        return ArticleRankWriteResult(**cypher_result.to_dict())

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        if G is None and projection_config is None:
            raise ValueError("Either 'G' or 'projection_config' must be provided")

        if G is not None:
            # Use graph name for estimation
            params = CallParameters(graph_name=G.name(), config={})
            cypher_result = self._query_runner.call_procedure(
                endpoint="gds.articleRank.stream.estimate", params=params
            ).squeeze()
        else:
            # Use projection config for estimation
            params = CallParameters(graph_name=projection_config, config={})
            cypher_result = self._query_runner.call_procedure(
                endpoint="gds.articleRank.stream.estimate", params=params
            ).squeeze()

        return EstimationResult(**cypher_result.to_dict())
