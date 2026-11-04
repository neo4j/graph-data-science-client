from __future__ import annotations

from abc import abstractmethod
from typing import Any

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.hashgnn_endpoints import (
    HashGNNEndpoints,
)


class HashGNNSessionEndpoints(HashGNNEndpoints):
    """HashGNN endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        iterations: int,
        embedding_density: int,
        *,
        output_dimension: int | None = None,
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        result_as_vector: bool = False,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        iterations
            Number of iterations to run.
        embedding_density
            The density of the generated embeddings (number of bits per embedding)
        output_dimension
            The dimension of the output embeddings
        neighbor_influence
            The influence of neighboring nodes
        generate_features
            Configuration for generating synthetic features from existing node properties
        binarize_features
            Configuration for binarizing continuous features
        heterogeneous
            Whether to use heterogeneous node processing for different node types
        feature_properties
            Names of the node properties to use as input features. Defaults to [] if not specified
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        result_as_vector
            Whether to produce the embeddings as vector values instead of lists of floats. Requires GDS 2026.11 or later.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
