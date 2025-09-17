from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.eigenvector_endpoints import (
    EigenvectorEndpoints,
    EigenvectorMutateResult,
    EigenvectorStatsResult,
    EigenvectorWriteResult,
)
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class EigenvectorCypherEndpoints(EigenvectorEndpoints):
    """Cypher-based implementation of Eigenvector Centrality algorithm endpoints."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> EigenvectorMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler,
            relationshipWeightProperty=relationship_weight_property,
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

        result = self._query_runner.call_procedure(endpoint="gds.eigenvector.mutate", params=params).squeeze()
        return EigenvectorMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> EigenvectorStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler,
            relationshipWeightProperty=relationship_weight_property,
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

        result = self._query_runner.call_procedure(endpoint="gds.eigenvector.stats", params=params).squeeze()
        return EigenvectorStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler,
            relationshipWeightProperty=relationship_weight_property,
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

        return self._query_runner.call_procedure(endpoint="gds.eigenvector.stream", params=params)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> EigenvectorWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler,
            relationshipWeightProperty=relationship_weight_property,
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

        result = self._query_runner.call_procedure(endpoint="gds.eigenvector.write", params=params).squeeze()
        return EigenvectorWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            concurrency=concurrency,
        )

        return estimate_algorithm(
            endpoint="gds.eigenvector.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
