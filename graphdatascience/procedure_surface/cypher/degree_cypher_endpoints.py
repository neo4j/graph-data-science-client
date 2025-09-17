from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import Graph

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.degree_endpoints import DegreeEndpoints, DegreeMutateResult, DegreeStatsResult, DegreeWriteResult
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter
from .estimation_utils import estimate_algorithm


class DegreeCypherEndpoints(DegreeEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DegreeMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            orientation=orientation,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            relationshipWeightProperty=relationship_weight_property,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.degree.mutate", params=params).squeeze()
        return DegreeMutateResult(**result.to_dict())

    def stats(
        self,
        G: Graph,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DegreeStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            orientation=orientation,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            relationshipWeightProperty=relationship_weight_property,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.degree.stats", params=params).squeeze()
        return DegreeStatsResult(**result.to_dict())

    def stream(
        self,
        G: Graph,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            orientation=orientation,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            relationshipWeightProperty=relationship_weight_property,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.degree.stream", params=params)

    def write(
        self,
        G: Graph,
        write_property: str,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
    ) -> DegreeWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            orientation=orientation,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            relationshipWeightProperty=relationship_weight_property,
            writeConcurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.degree.write", params=params).squeeze()
        return DegreeWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            orientation=orientation,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
        )
        return estimate_algorithm(
            endpoint="gds.degree.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
