from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import Node2VecEndpoints, Node2VecMutateResult, Node2VecWriteResult
from ..utils.config_converter import ConfigConverter


class Node2VecCypherEndpoints(Node2VecEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
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

        result = self._query_runner.call_procedure(endpoint="gds.node2vec.mutate", params=params).squeeze()

        return Node2VecMutateResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
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

        return self._query_runner.call_procedure(endpoint="gds.node2vec.stream", params=params)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
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

        result = self._query_runner.call_procedure(endpoint="gds.node2vec.write", params=params).squeeze()

        return Node2VecWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[GraphV2, Dict[str, Any]],
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
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
