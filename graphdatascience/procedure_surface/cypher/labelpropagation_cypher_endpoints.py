from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.labelpropagation_endpoints import (
    LabelPropagationEndpoints,
    LabelPropagationMutateResult,
    LabelPropagationStatsResult,
    LabelPropagationWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class LabelPropagationCypherEndpoints(LabelPropagationEndpoints):
    """
    Implementation of the Label Propagation algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> LabelPropagationMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.labelPropagation.mutate", params=params, logging=log_progress
        ).squeeze()

        return LabelPropagationMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> LabelPropagationStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.labelPropagation.stats", params=params, logging=log_progress
        ).squeeze()

        return LabelPropagationStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.labelPropagation.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> LabelPropagationWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            min_community_size=min_community_size,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.labelPropagation.write", params=params, logging=log_progress
        ).squeeze()  # type: ignore

        return LabelPropagationWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            max_iterations=max_iterations,
            node_labels=node_labels,
            node_weight_property=node_weight_property,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
        )
        return estimate_algorithm(
            endpoint="gds.labelPropagation.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
