from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.hdbscan_endpoints import (
    HdbscanEndpoints,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class HdbscanSessionEndpoints(HdbscanEndpoints):
    """Hdbscan endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
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
        leaf_size
            The maximum leaf size of the tree structure used in the algorithm
        samples
            The number of samples used for density estimation
        min_cluster_size
            The minimum size of clusters
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        job_id
            Identifier for the computation.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
