from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.centrality.eigenvector_endpoints import (
    EigenvectorEndpoints,
    EigenvectorMutateResult,
    EigenvectorStatsResult,
    EigenvectorWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
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
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EigenvectorMutateResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler_value,
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
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EigenvectorStatsResult:
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler_value,
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
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler_value,
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
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> EigenvectorWriteResult:
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler_value,
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
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        algo_config = ConfigConverter.convert_to_gds_config(
            maxIterations=max_iterations,
            tolerance=tolerance,
            sourceNodes=source_nodes,
            scaler=scaler_value,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            concurrency=concurrency,
        )

        return estimate_algorithm(
            endpoint="gds.eigenvector.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
