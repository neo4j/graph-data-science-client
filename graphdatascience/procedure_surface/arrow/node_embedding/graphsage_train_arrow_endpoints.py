from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.api.node_embedding.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
    GraphSageTrainResult,
)
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_predict_arrow_endpoints import (
    GraphSagePredictArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class GraphSageTrainArrowEndpoints(GraphSageTrainEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, write_back_client=write_back_client, show_progress=show_progress
        )
        self._model_api = ModelApiArrow(arrow_client)

    def __call__(
        self,
        G: GraphV2,
        model_name: str,
        feature_properties: list[str],
        *,
        activation_function: Any | None = None,
        negative_sample_weight: int | None = None,
        embedding_dimension: int | None = None,
        tolerance: float | None = None,
        learning_rate: float | None = None,
        max_iterations: int | None = None,
        sample_sizes: list[int] | None = None,
        aggregator: Any | None = None,
        penalty_l2: float | None = None,
        search_depth: int | None = None,
        epochs: int | None = None,
        projected_feature_dimension: int | None = None,
        batch_sampling_ratio: float | None = None,
        store_model_to_disk: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
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

    def estimate(
        self,
        G: GraphV2,
        model_name: str,
        feature_properties: list[str],
        *,
        activation_function: Any | None = None,
        negative_sample_weight: int | None = None,
        embedding_dimension: int | None = None,
        tolerance: float | None = None,
        learning_rate: float | None = None,
        max_iterations: int | None = None,
        sample_sizes: list[int] | None = None,
        aggregator: Any | None = None,
        penalty_l2: float | None = None,
        search_depth: int | None = None,
        epochs: int | None = None,
        projected_feature_dimension: int | None = None,
        batch_sampling_ratio: float | None = None,
        store_model_to_disk: bool | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> EstimationResult:
        return self._node_property_endpoints.estimate(
            estimate_endpoint="v2/embeddings.graphSage.train.estimate",
            G=G,
            algo_config=self._node_property_endpoints.create_estimate_config(
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
            ),
        )
