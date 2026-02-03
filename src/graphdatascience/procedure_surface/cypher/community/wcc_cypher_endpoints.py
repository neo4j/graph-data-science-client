from typing import Any

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.wcc_endpoints import (
    WccEndpoints,
    WccMutateResult,
    WccStatsResult,
    WccWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ....call_parameters import CallParameters
from ....query_runner.query_runner import QueryRunner
from ...api.estimation_result import EstimationResult
from ...utils.config_converter import ConfigConverter


class WccCypherEndpoints(WccEndpoints):
    """
    Implementation of the Weakly Connected Components (WCC) algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        threshold: float = 0.0,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> WccMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.wcc.mutate", params=params, logging=log_progress
        ).squeeze()

        return WccMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        threshold: float = 0.0,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> WccStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.wcc.stats", params=params, logging=log_progress
        ).squeeze()

        return WccStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        min_component_size: int | None = None,
        threshold: float = 0.0,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.wcc.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        min_component_size: int | None = None,
        threshold: float = 0.0,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
        write_concurrency: int | None = None,
    ) -> WccWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.wcc.write", params=params, logging=log_progress
        ).squeeze()  # type: ignore

        return WccWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        threshold: float = 0.0,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            threshold=threshold,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            seed_property=seed_property,
            consecutive_ids=consecutive_ids,
            relationship_weight_property=relationship_weight_property,
        )
        return estimate_algorithm(
            endpoint="gds.wcc.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
