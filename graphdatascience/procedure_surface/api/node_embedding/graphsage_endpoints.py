from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.api.node_embedding.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
    GraphSageTrainResult,
)


class GraphSageEndpoints(GraphSageTrainEndpoints, GraphSagePredictEndpoints):
    """
    API for the GraphSage algorithm, combining both training and prediction functionalities.
    """

    def __init__(
        self,
        train_endpoints: GraphSageTrainEndpoints,
        predict_endpoints: GraphSagePredictEndpoints,
    ) -> None:
        self._train_endpoints = train_endpoints
        self._predict_endpoints = predict_endpoints

    def train(
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
        return self._train_endpoints.train(
            G,
            model_name,
            feature_properties,
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
        return self._predict_endpoints.stream(
            G,
            model_name,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

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
        return self._predict_endpoints.write(
            G,
            model_name,
            write_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

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
        return self._predict_endpoints.mutate(
            G,
            model_name,
            mutate_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

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
        return self._predict_endpoints.estimate(
            G,
            model_name,
            relationship_types=relationship_types,
            node_labels=node_labels,
            batch_size=batch_size,
            concurrency=concurrency,
            log_progress=log_progress,
            username=username,
            sudo=sudo,
            job_id=job_id,
        )
