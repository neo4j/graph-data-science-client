from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import (
    Node2VecEndpoints,
    Node2VecMutateResult,
    Node2VecWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class Node2VecArrowEndpoints(Node2VecEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/embeddings.node2vec", config, mutate_property)
        result["postProcessingMillis"] = 0  # node2vec always returns 0 for post processing time

        return Node2VecMutateResult(**result)

    def stream(
        self,
        G: GraphV2,
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
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
        G: GraphV2,
        write_property: str,
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        write_concurrency: int | None = None,
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
            "v2/embeddings.node2vec",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return Node2VecWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
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
