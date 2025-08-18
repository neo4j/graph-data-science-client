from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.estimation_result import EstimationResult
from ..api.node2vec_endpoints import Node2VecEndpoints, Node2VecMutateResult, Node2VecWriteResult
from .node_property_endpoints import NodePropertyEndpoints


class Node2VecArrowEndpoints(Node2VecEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
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
        config = self._node_property_endpoints.create_base_config(
            G,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/embeddings.node2vec", G, config, mutate_property)

        return Node2VecMutateResult(**result)

    def stream(
        self,
        G: Graph,
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
        config = self._node_property_endpoints.create_base_config(
            G,
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

        return self._node_property_endpoints.run_job_and_stream("v2/embeddings.node2vec", G, config)

    def write(
        self,
        G: Graph,
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
        config = self._node_property_endpoints.create_base_config(
            G,
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
            write_property=write_property,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.node2vec", G, config, write_concurrency, concurrency
        )

        return Node2VecWriteResult(**result)

    def estimate(
        self,
        G: Union[Graph, Dict[str, Any]],
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
        config = self._node_property_endpoints.create_estimate_config(
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

        return self._node_property_endpoints.estimate("v2/embeddings.node2vec.estimate", G, config)
