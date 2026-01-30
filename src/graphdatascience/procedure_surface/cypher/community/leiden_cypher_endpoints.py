from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.leiden_endpoints import (
    LeidenEndpoints,
    LeidenMutateResult,
    LeidenStatsResult,
    LeidenWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


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
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
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
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
        write_concurrency: int | None = None,
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
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        theta: float = 0.01,
        tolerance: float = 0.0001,
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
