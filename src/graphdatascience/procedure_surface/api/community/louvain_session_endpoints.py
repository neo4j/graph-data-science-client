from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.louvain_endpoints import (
    LouvainEndpoints,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class LouvainSessionEndpoints(LouvainEndpoints):
    """Louvain endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        tolerance: float = 0.0001,
        max_levels: int = 10,
        include_intermediate_communities: bool = False,
        max_iterations: int = 10,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        tolerance
            Minimum change in scores between iterations.
        max_levels
            The maximum number of levels in the hierarchy
        include_intermediate_communities
            Whether to include intermediate community assignments
        max_iterations
            Maximum number of iterations to run per level.
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
        seed_property
            Name of the property to be used to for the initial value of a node.
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
