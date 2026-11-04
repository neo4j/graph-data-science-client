from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_endpoints import (
    SourceTargetAStarEndpoints,
)


class SourceTargetAStarSessionEndpoints(SourceTargetAStarEndpoints):
    """A* shortest path endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        *,
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
        Starts an A* shortest path computation from a source node to a target node as a job and
        returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the paths afterwards.

        Parameters
        ----------
        G
           Graph object to use
        source_node
            Node id to use as the starting point.
        target_node
            Node id to use as the destination.
        latitude_property
            Name of the node property that stores the latitude value.
        longitude_property
            Name of the node property that stores the longitude value.
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
