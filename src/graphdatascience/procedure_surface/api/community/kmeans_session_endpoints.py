from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import (
    KMeansEndpoints,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class KMeansSessionEndpoints(KMeansEndpoints):
    """KMeans endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        node_property
            The node property to use for clustering
        compute_silhouette
            Whether to compute silhouette coefficient
        concurrency
            Number of concurrent threads to use.
        delta_threshold
            Minimum change between iterations.
        initial_sampler
            Sampling method for initial centroids.
        job_id
            Identifier for the computation.
        k
            Number of clusters to form.
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        number_of_restarts
            The number of times the algorithm should be restarted with different initial centers
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        seed_centroids
            Initial centroids for the algorithm
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
