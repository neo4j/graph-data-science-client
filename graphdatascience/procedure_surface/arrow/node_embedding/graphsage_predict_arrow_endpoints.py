from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class GraphSagePredictArrowEndpoints(GraphSagePredictEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )
        self._model_api = ModelApiArrow(arrow_client)

    def stream(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            modelName=model_name,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )
        return self._node_property_endpoints.run_job_and_stream("v2/embeddings.graphSage", G, config)

    def write(
        self,
        G: GraphV2,
        model_name: str,
        write_property: str,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        write_concurrency: int | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
    ) -> GraphSageWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            modelName=model_name,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        raw_result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.graphSage",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return GraphSageWriteResult(**raw_result)

    def mutate(
        self,
        G: GraphV2,
        model_name: str,
        mutate_property: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
    ) -> GraphSageMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            modelName=model_name,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        raw_result = self._node_property_endpoints.run_job_and_mutate(
            "v2/embeddings.graphSage",
            config,
            mutate_property,
        )

        return GraphSageMutateResult(**raw_result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        model_name: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            modelName=model_name,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            username=username,
            logProgress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            jobId=job_id,
            batchSize=batch_size,
        )

        return self._node_property_endpoints.estimate(
            "v2/embeddings.graphSage.estimate",
            G,
            config,
        )
