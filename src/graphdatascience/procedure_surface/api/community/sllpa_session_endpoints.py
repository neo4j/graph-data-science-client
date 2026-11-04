from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import (
    SllpaEndpoints,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class SllpaSessionEndpoints(SllpaEndpoints):
    """Sllpa endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float = 0.2,
        node_labels: list[str] = ALL_LABELS,
        partitioning: str = "RANGE",
        relationship_types: list[str] = ALL_TYPES,
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
        max_iterations
            Maximum number of iterations to run.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        min_association_strength
            Minimum association strength for community assignment
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        partitioning
            Partitioning configuration for the algorithm
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
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
