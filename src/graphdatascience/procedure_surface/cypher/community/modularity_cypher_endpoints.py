from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.modularity_endpoints import (
    ModularityEndpoints,
    ModularityStatsResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ....call_parameters import CallParameters
from ....query_runner.query_runner import QueryRunner
from ...utils.config_converter import ConfigConverter


class ModularityCypherEndpoints(ModularityEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stats(
        self,
        G: GraphV2,
        community_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> ModularityStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            community_property=community_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.modularity.stats", params=params, logging=log_progress
        ).iloc[0]

        return ModularityStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        community_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            community_property=community_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.modularity.stream", params=params, logging=log_progress)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        community_property: str,
        *,
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            community_property=community_property,
            concurrency=concurrency,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
        )
        return estimate_algorithm(
            endpoint="gds.modularity.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
