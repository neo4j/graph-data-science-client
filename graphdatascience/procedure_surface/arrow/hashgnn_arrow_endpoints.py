from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import (
    HashGNNEndpoints,
    HashGNNMutateResult,
    HashGNNWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class HashGNNArrowEndpoints(HashGNNEndpoints):
    """
    Implementation of the HashGNN algorithm endpoints using Arrow Flight backend.
    This class handles the actual execution by forwarding calls to the node property endpoints.
    """

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        mutate_property: str,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/embeddings.hashgnn", G, config, mutate_property)

        return HashGNNMutateResult(**result)

    def stream(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
        random_seed: Optional[int] = None,
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
            "v2/embeddings.hashgnn", G, config, write_concurrency, concurrency, write_property
        )

        return HashGNNWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        iterations: int,
        embedding_density: int,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
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
