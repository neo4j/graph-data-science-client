from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.clique_counting_endpoints import (
    CliqueCountingEndpoints,
    CliqueCountingMutateResult,
    CliqueCountingStatsResult,
    CliqueCountingWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class CliqueCountingCypherEndpoints(CliqueCountingEndpoints):
    """
    Implementation of the Clique Counting algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> CliqueCountingMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.cliqueCounting.mutate", params=params, logging=log_progress
        ).squeeze()

        return CliqueCountingMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> CliqueCountingStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.cliqueCounting.stats", params=params, logging=log_progress
        ).squeeze()

        return CliqueCountingStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.cliqueCounting.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        write_property: str,
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> CliqueCountingWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.cliqueCounting.write", params=params, logging=log_progress
        ).squeeze()  # type: ignore

        return CliqueCountingWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        concurrency: Optional[int] = 4,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            node_labels=node_labels,
            relationship_types=relationship_types,
        )
        return estimate_algorithm(
            endpoint="gds.cliqueCounting.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
