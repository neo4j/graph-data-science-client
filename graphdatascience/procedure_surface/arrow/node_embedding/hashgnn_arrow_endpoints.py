from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import (
    HashGNNEndpoints,
    HashGNNMutateResult,
    HashGNNWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class HashGNNArrowEndpoints(HashGNNEndpoints):
    """
    Implementation of the HashGNN algorithm endpoints using Arrow Flight backend.
    This class handles the actual execution by forwarding calls to the node property endpoints.
    """

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
        iterations: int,
        embedding_density: int,
        mutate_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> HashGNNMutateResult:
        """
        Compute node embeddings using HashGNN and store the result as a node property in the in-memory graph.
        """

        config = self._node_property_endpoints.create_base_config(
            G,
            iterations=iterations,
            embedding_density=embedding_density,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/embeddings.hashgnn", config, mutate_property)

        return HashGNNMutateResult(**result)

    def stream(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> DataFrame:
        """
        Compute node embeddings using HashGNN and stream the results.
        """

        config = self._node_property_endpoints.create_base_config(
            G,
            iterations=iterations,
            embedding_density=embedding_density,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/embeddings.hashgnn", G, config)

    def write(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        write_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: int | None = None,
        random_seed: int | None = None,
    ) -> HashGNNWriteResult:
        """
        Compute node embeddings using HashGNN and write the results to the Neo4j database.
        """

        config = self._node_property_endpoints.create_base_config(
            G,
            iterations=iterations,
            embedding_density=embedding_density,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            write_concurrency=write_concurrency,
            random_seed=random_seed,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.hashgnn",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return HashGNNWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        iterations: int,
        embedding_density: int,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the cost of running HashGNN on the given graph.
        """

        config = self._node_property_endpoints.create_estimate_config(
            iterations=iterations,
            embedding_density=embedding_density,
            output_dimension=output_dimension,
            neighbor_influence=neighbor_influence,
            generate_features=generate_features,
            binarize_features=binarize_features,
            heterogeneous=heterogeneous,
            feature_properties=feature_properties,
            random_seed=random_seed,
        )

        return self._node_property_endpoints.estimate("v2/embeddings.hashgnn.estimate", G, config)
