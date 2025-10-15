from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.api.node_embedding.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
    GraphSageTrainResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.cypher.model_api_cypher import ModelApiCypher
from graphdatascience.procedure_surface.cypher.node_embedding.graphsage_predict_cypher_endpoints import (
    GraphSagePredictCypherEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class GraphSageTrainCypherEndpoints(GraphSageTrainEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.beta.graphSage.train", params=params, logging=log_progress
        ).iloc[0]

        return GraphSageModelV2(
            name=model_name,
            model_api=ModelApiCypher(self._query_runner),
            predict_endpoints=GraphSagePredictCypherEndpoints(self._query_runner),
        ), GraphSageTrainResult(**result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
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

        return estimate_algorithm("gds.beta.graphSage.train.estimate", self._query_runner, G, config)
