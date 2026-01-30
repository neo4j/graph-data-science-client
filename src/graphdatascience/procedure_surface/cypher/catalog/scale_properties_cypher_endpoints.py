from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scale_properties_endpoints import (
    ScalePropertiesEndpoints,
    ScalePropertiesMutateResult,
    ScalePropertiesStatsResult,
    ScalePropertiesWriteResult,
)
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class ScalePropertiesCypherEndpoints(ScalePropertiesEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ScalePropertiesMutateResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            nodeProperties=node_properties,
            scaler=scaler_value,
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
            endpoint="gds.scaleProperties.mutate", params=params, logging=log_progress
        ).squeeze()
        return ScalePropertiesMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ScalePropertiesStatsResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            scaler=scaler_value,
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
            endpoint="gds.scaleProperties.stats", params=params, logging=log_progress
        ).squeeze()
        return ScalePropertiesStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            scaler=scaler_value,
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

        return self._query_runner.call_procedure(
            endpoint="gds.scaleProperties.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        write_property: str,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> ScalePropertiesWriteResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            nodeProperties=node_properties,
            scaler=scaler_value,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.scaleProperties.write", params=params, logging=log_progress
        ).squeeze()
        return ScalePropertiesWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        algo_config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            scaler=scaler_value,
            nodeLabels=node_labels,
            concurrency=concurrency,
        )
        return estimate_algorithm(
            endpoint="gds.scaleProperties.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
