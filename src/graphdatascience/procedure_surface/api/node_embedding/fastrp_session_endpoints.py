from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import FastRPEndpoints


class FastRPSessionEndpoints(FastRPEndpoints):
    """FastRP endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        embedding_dimension: int,
        *,
        iteration_weights: list[float] = [0.0, 1.0, 1.0],
        normalization_strength: float = 0.0,
        node_self_influence: float = 0.0,
        property_ratio: float = 0.0,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
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
        embedding_dimension
            Output dimensionality of the embeddings
        iteration_weights
            Weights for each iteration. Controls the influence of each iteration on the final embedding.
        normalization_strength
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence
            The influence of the node's own features on its embedding
        property_ratio
            The ratio of node properties to use in the embedding
        feature_properties
            List of node properties to use as features in the embedding.
            Defaults to [] if not specified
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
