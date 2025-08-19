from typing import Any, List, Optional

from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2
from graphdatascience.procedure_surface.cypher.graphsage_predict_cypher_endpoints import GraphSagePredictCypherEndpoints
from graphdatascience.procedure_surface.cypher.model_api_cypher import ModelApiCypher

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
    GraphSageTrainResult,
)
from ..utils.config_converter import ConfigConverter


class GraphSageTrainCypherEndpoints(GraphSageTrainEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def train(
        self,
        G: Graph,
        model_name: str,
        feature_properties: List[str],
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

        result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.train", params=params).iloc[0]

        return GraphSageModelV2(
            name=model_name,
            model_api=ModelApiCypher(self._query_runner),
            predict_endpoints=GraphSagePredictCypherEndpoints(self._query_runner),
        ), GraphSageTrainResult(**result.to_dict())
