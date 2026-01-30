from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.maxkcut_endpoints import (
    MaxKCutEndpoints,
    MaxKCutMutateResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class MaxKCutCypherEndpoints(MaxKCutEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        iterations: int = 8,
        job_id: str | None = None,
        k: int = 2,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        vns_max_neighborhood_order: int = 0,
    ) -> MaxKCutMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            iterations=iterations,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            mutate_property=mutate_property,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
            vns_max_neighborhood_order=vns_max_neighborhood_order,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.maxkcut.mutate", params=params).squeeze()

        return MaxKCutMutateResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        iterations: int = 8,
        job_id: str | None = None,
        k: int = 2,
        log_progress: bool = True,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        vns_max_neighborhood_order: int = 0,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            iterations=iterations,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            min_community_size=min_community_size,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
            vns_max_neighborhood_order=vns_max_neighborhood_order,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.maxkcut.stream", params=params)

        return result

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        iterations: int = 8,
        k: int = 2,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        vns_max_neighborhood_order: int = 0,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            iterations=iterations,
            k=k,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            vns_max_neighborhood_order=vns_max_neighborhood_order,
        )

        return estimate_algorithm("gds.maxkcut.stream.estimate", self._query_runner, G, config)
