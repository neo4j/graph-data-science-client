from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.similarity.node_similarity_endpoints import (
    NodeSimilarityEndpoints,
)
from graphdatascience.procedure_surface.api.similarity.node_similarity_filtered_session_endpoints import (
    NodeSimilarityFilteredSessionEndpoints,
)


class NodeSimilaritySessionEndpoints(NodeSimilarityEndpoints):
    """NodeSimilarity endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        top_k
            Number of most similar nodes to return for each node.
        bottom_k
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff
            The threshold for similarity scores.
        degree_cutoff
            The minimum degree a node must have to be considered.
        upper_degree_cutoff
            The maximum degree a node can have to be considered.
        similarity_metric
            The similarity metric to use for computation. JACCARD, OVERLAP or COSINE.
        use_components
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
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

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass

    @property
    @abstractmethod
    def filtered(self) -> NodeSimilarityFilteredSessionEndpoints:
        """Access to the filtered Node Similarity endpoints."""
        ...
