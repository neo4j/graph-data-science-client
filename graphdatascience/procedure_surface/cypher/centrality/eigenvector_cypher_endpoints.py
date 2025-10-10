from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.eigenvector_endpoints import (
    EigenvectorEndpoints,
    EigenvectorMutateResult,
    EigenvectorStatsResult,
    EigenvectorWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class EigenvectorCypherEndpoints(EigenvectorEndpoints):
    """Cypher-based implementation of Eigenvector Centrality algorithm endpoints."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.eigenvector.mutate", params=params, logging=log_progress
        ).squeeze()
        return EigenvectorMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.eigenvector.stats", params=params, logging=log_progress
        ).squeeze()
        return EigenvectorStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        return self._query_runner.call_procedure(endpoint="gds.eigenvector.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.eigenvector.write", params=params, logging=log_progress
        ).squeeze()
        return EigenvectorWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
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
