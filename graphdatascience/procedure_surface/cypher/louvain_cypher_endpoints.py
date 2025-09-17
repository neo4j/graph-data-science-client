from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..api.louvain_endpoints import LouvainEndpoints, LouvainMutateResult, LouvainStatsResult, LouvainWriteResult
from ..utils.config_converter import ConfigConverter


class LouvainCypherEndpoints(LouvainEndpoints):
    """
    Implementation of the Louvain algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> LouvainMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.louvain.mutate", params=params).squeeze()

        return LouvainMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> LouvainStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.louvain.stats", params=params).squeeze()

        return LouvainStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        min_community_size: Optional[int] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.louvain.stream", params=params)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        min_community_size: Optional[int] = None,
    ) -> LouvainWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            tolerance=tolerance,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.louvain.write", params=params).squeeze()  # type: ignore

        return LouvainWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            tolerance=tolerance,
            max_levels=max_levels,
            include_intermediate_communities=include_intermediate_communities,
            max_iterations=max_iterations,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            seed_property=seed_property,
            consecutive_ids=consecutive_ids,
            relationship_weight_property=relationship_weight_property,
        )
        return estimate_algorithm(
            endpoint="gds.louvain.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
