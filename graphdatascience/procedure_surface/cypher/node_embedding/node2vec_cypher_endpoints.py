from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import (
    Node2VecEndpoints,
    Node2VecMutateResult,
    Node2VecWriteResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class Node2VecCypherEndpoints(Node2VecEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> Node2VecMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            iterations=iterations,
            negative_sampling_rate=negative_sampling_rate,
            positive_sampling_factor=positive_sampling_factor,
            embedding_dimension=embedding_dimension,
            embedding_initializer=embedding_initializer,
            initial_learning_rate=initial_learning_rate,
            min_learning_rate=min_learning_rate,
            window_size=window_size,
            negative_sampling_exponent=negative_sampling_exponent,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
            in_out_factor=in_out_factor,
            return_factor=return_factor,
            walk_buffer_size=walk_buffer_size,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.node2vec.mutate", params=params, logging=log_progress
        ).squeeze()

        return Node2VecMutateResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            iterations=iterations,
            negative_sampling_rate=negative_sampling_rate,
            positive_sampling_factor=positive_sampling_factor,
            embedding_dimension=embedding_dimension,
            embedding_initializer=embedding_initializer,
            initial_learning_rate=initial_learning_rate,
            min_learning_rate=min_learning_rate,
            window_size=window_size,
            negative_sampling_exponent=negative_sampling_exponent,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
            in_out_factor=in_out_factor,
            return_factor=return_factor,
            walk_buffer_size=walk_buffer_size,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.node2vec.stream", params=params, logging=log_progress)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
        write_concurrency: Any | None = None,
    ) -> Node2VecWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            iterations=iterations,
            negative_sampling_rate=negative_sampling_rate,
            positive_sampling_factor=positive_sampling_factor,
            embedding_dimension=embedding_dimension,
            embedding_initializer=embedding_initializer,
            initial_learning_rate=initial_learning_rate,
            min_learning_rate=min_learning_rate,
            window_size=window_size,
            negative_sampling_exponent=negative_sampling_exponent,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
            in_out_factor=in_out_factor,
            return_factor=return_factor,
            walk_buffer_size=walk_buffer_size,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.node2vec.write", params=params, logging=log_progress
        ).squeeze()

        return Node2VecWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            iterations=iterations,
            negative_sampling_rate=negative_sampling_rate,
            positive_sampling_factor=positive_sampling_factor,
            embedding_dimension=embedding_dimension,
            embedding_initializer=embedding_initializer,
            initial_learning_rate=initial_learning_rate,
            min_learning_rate=min_learning_rate,
            window_size=window_size,
            negative_sampling_exponent=negative_sampling_exponent,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            walk_length=walk_length,
            walks_per_node=walks_per_node,
            in_out_factor=in_out_factor,
            return_factor=return_factor,
            walk_buffer_size=walk_buffer_size,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        graph_name = G.name() if isinstance(G, GraphV2) else None
        params = CallParameters(graph_name=graph_name, config=config)

        result = self._query_runner.call_procedure(endpoint="gds.node2vec.stream.estimate", params=params).squeeze()

        return EstimationResult(**result.to_dict())
