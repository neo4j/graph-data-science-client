from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from .model_api_arrow import ModelApiArrow
from .node_property_endpoints import NodePropertyEndpoints


class GraphSagePredictArrowEndpoints(GraphSagePredictEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client = arrow_client
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client)
        self._model_api = ModelApiArrow(arrow_client)

    def stream(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
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
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
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
            write_concurrency,
            concurrency,
            write_property
        )

        return GraphSageWriteResult(**raw_result)

    def mutate(
        self,
        G: GraphV2,
        model_name: str,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
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
            G,
            config,
            mutate_property,
        )

        return GraphSageMutateResult(**raw_result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        model_name: str,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
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
