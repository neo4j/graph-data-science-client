from typing import Any, List, Optional

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.arrow.graphsage_predict_arrow_endpoints import GraphSagePredictArrowEndpoints

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ...graph.graph_object import Graph
from ..api.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
    GraphSageTrainResult,
)
from .model_api_arrow import ModelApiArrow
from .node_property_endpoints import NodePropertyEndpoints


class GraphSageTrainArrowEndpoints(GraphSageTrainEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient]):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client=write_back_client)
        self._model_api = ModelApiArrow(arrow_client)

    def train(
        self,
        G: GraphV2,
        model_name: str,
        feature_properties: List[str],
        *,
        activation_function: Optional[Any] = None,
        negative_sample_weight: Optional[int] = None,
        embedding_dimension: Optional[int] = None,
        tolerance: Optional[float] = None,
        learning_rate: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_sizes: Optional[List[int]] = None,
        aggregator: Optional[Any] = None,
        penalty_l2: Optional[float] = None,
        search_depth: Optional[int] = None,
        epochs: Optional[int] = None,
        projected_feature_dimension: Optional[int] = None,
        batch_sampling_ratio: Optional[float] = None,
        store_model_to_disk: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> tuple[GraphSageModelV2, GraphSageTrainResult]:
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            feature_properties=feature_properties,
            activation_function=activation_function,
            negative_sample_weight=negative_sample_weight,
            embedding_dimension=embedding_dimension,
            tolerance=tolerance,
            learning_rate=learning_rate,
            max_iterations=max_iterations,
            sample_sizes=sample_sizes,
            aggregator=aggregator,
            penalty_l2=penalty_l2,
            search_depth=search_depth,
            epochs=epochs,
            projected_feature_dimension=projected_feature_dimension,
            batch_sampling_ratio=batch_sampling_ratio,
            store_model_to_disk=store_model_to_disk,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        result = self._node_property_endpoints.run_job_and_get_summary("v2/embeddings.graphSage.train", G, config)

        model = GraphSageModelV2(
            model_name,
            self._model_api,
            predict_endpoints=GraphSagePredictArrowEndpoints(self._arrow_client, self._write_back_client),
        )
        train_result = GraphSageTrainResult(**result)

        return model, train_result
