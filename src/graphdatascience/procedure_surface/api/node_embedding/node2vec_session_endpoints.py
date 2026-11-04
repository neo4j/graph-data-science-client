from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.node2vec_endpoints import (
    Node2VecEndpoints,
)


class Node2VecSessionEndpoints(Node2VecEndpoints):
    """Node2Vec endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
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
        negative_sampling_rate
            Number of negative samples for each positive sample
        positive_sampling_factor
            Factor to multiply positive sampling weights
        embedding_dimension
            Output dimensionality of the embeddings
        embedding_initializer
            Strategy for initializing node embeddings. Either "UNIFORM" or "NORMALIZED"
        initial_learning_rate
            The initial learning rate
        min_learning_rate
            The minimum learning rate
        window_size
            Size of the context window
        negative_sampling_exponent
            Exponent for negative sampling probability distribution
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        walk_length
            The length of each random walk
        walks_per_node
            Number of walks to sample for each node
        in_out_factor
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size
            Buffer size for walk sampling
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.
        result_as_vector
            Whether to produce the embeddings as vector values instead of lists of floats.
            Requires GDS 2026.11 or later.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
