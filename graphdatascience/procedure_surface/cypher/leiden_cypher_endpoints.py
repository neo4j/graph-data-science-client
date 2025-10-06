from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.leiden_endpoints import (
    LeidenEndpoints,
    LeidenMutateResult,
    LeidenStatsResult,
    LeidenWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class LeidenCypherEndpoints(LeidenEndpoints):
    """
    Implementation of the Leiden algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> LeidenMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.leiden.mutate", params=params, logging=log_progress
        ).squeeze()

        return LeidenMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> LeidenStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.leiden.stats", params=params, logging=log_progress
        ).squeeze()

        return LeidenStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.leiden.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> LeidenWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            job_id=job_id,
            log_progress=log_progress,
            max_levels=max_levels,
            min_community_size=min_community_size,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            theta=theta,
            tolerance=tolerance,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.leiden.write", params=params, logging=log_progress
        ).squeeze()

        return LeidenWriteResult(**cypher_result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            gamma=gamma,
            include_intermediate_communities=include_intermediate_communities,
            max_levels=max_levels,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            theta=theta,
            tolerance=tolerance,
        )

        return estimate_algorithm(
            endpoint="gds.leiden.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=config,
        )
