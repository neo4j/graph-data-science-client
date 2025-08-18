from typing import Any, List, Optional

from pandas import DataFrame

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.graphsage_endpoints import (
    GraphSageEndpoints,
    GraphSageMutateResult,
    GraphSageTrainResult,
    GraphSageWriteResult,
)
from ..utils.config_converter import ConfigConverter


class GraphSageCypherEndpoints(GraphSageEndpoints):
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
    ) -> GraphSageTrainResult:
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

        result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.train", params=params).squeeze()

        return GraphSageTrainResult(**result.to_dict())

    def mutate(
        self,
        G: Graph,
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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            mutate_property=mutate_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.mutate", params=params).squeeze()

        return GraphSageMutateResult(**result.to_dict())

    def stream(
        self,
        G: Graph,
        model_name: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.beta.graphSage.stream", params=params)

    def write(
        self,
        G: Graph,
        model_name: str,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
        write_concurrency: Optional[Any] = None,
    ) -> GraphSageWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            write_property=write_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.beta.graphSage.write", params=params).squeeze()

        return GraphSageWriteResult(**result.to_dict())
