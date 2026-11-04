from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import (
    AllShortestPathEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_session_endpoints import (
    DeltaSteppingSessionEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.single_source_dijkstra_session_endpoints import (
    SingleSourceDijkstraSessionEndpoints,
)


class AllShortestPathSessionEndpoints(AllShortestPathEndpoints):
    """AllShortestPath endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
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
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
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

    @property
    @abstractmethod
    def delta(self) -> DeltaSteppingSessionEndpoints:
        """Access to Delta Stepping shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def dijkstra(self) -> SingleSourceDijkstraSessionEndpoints:
        """Access to Dijkstra shortest path algorithm endpoints."""
        ...
