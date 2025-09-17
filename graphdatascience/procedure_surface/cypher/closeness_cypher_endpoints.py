from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.closeness_endpoints import (
    ClosenessEndpoints,
    ClosenessMutateResult,
    ClosenessStatsResult,
    ClosenessWriteResult,
)
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class ClosenessCypherEndpoints(ClosenessEndpoints):
    """Cypher-based implementation of Closeness Centrality algorithm endpoints."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            useWassermanFaust=use_wasserman_faust,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.closeness.mutate", params=params).squeeze()
        return ClosenessMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            useWassermanFaust=use_wasserman_faust,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.closeness.stats", params=params).squeeze()
        return ClosenessStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            useWassermanFaust=use_wasserman_faust,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.closeness.stream", params=params)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> ClosenessWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            useWassermanFaust=use_wasserman_faust,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.closeness.write", params=params).squeeze()
        return ClosenessWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            useWassermanFaust=use_wasserman_faust,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            concurrency=concurrency,
        )
        return estimate_algorithm(
            endpoint="gds.closeness.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
