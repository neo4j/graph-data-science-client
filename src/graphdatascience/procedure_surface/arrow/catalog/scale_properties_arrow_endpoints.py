from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
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
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class ScalePropertiesArrowEndpoints(ScalePropertiesEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

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

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            sudo=sudo,
            node_properties=node_properties,
            scaler=scaler_value,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/graph.nodeProperties.scale", config, mutate_property
        )

        return ScalePropertiesMutateResult(**result)

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

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            sudo=sudo,
            node_properties=node_properties,
            scaler=scaler_value,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/graph.nodeProperties.scale", config
        )

        return ScalePropertiesStatsResult(**computation_result)

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

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            sudo=sudo,
            node_properties=node_properties,
            scaler=scaler_value,
        )

        result = self._node_property_endpoints.run_job_and_stream("v2/graph.nodeProperties.scale", G, config)
        result.rename(columns={"scaledProperties": "scaledProperty"}, inplace=True)

        return result

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

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            sudo=sudo,
            node_properties=node_properties,
            scaler=scaler_value,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/graph.nodeProperties.scale",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return ScalePropertiesWriteResult(**result)

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

        config = self._node_property_endpoints.create_estimate_config(
            node_labels=node_labels,
            concurrency=concurrency,
            node_properties=node_properties,
            scaler=scaler_value,
        )
        return self._node_property_endpoints.estimate("v2/graph.nodeProperties.scale.estimate", G, config)
