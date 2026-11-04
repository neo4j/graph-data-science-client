from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.leiden_endpoints import (
    LeidenEndpoints,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class LeidenSessionEndpoints(LeidenEndpoints):
    """Leiden endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
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
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        gamma
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities
            Whether to include intermediate communities
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_levels
            The maximum number of levels
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property
            Name of the property to be used to for the initial value of a node.
        sudo
            Disable the memory guard.
        theta
            The theta parameter for the Leiden algorithm
        tolerance
            Minimum change in scores between iterations.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
