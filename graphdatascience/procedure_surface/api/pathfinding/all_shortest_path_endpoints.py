from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_endpoints import SingleSourceDeltaEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_dijkstra_endpoints import (
    SingleSourceDijkstraEndpoints,
)


class AllShortestPathEndpoints(ABC):
    """
    Container for shortest path algorithm endpoints from a given source node.
    Provides access to different shortest path algorithms such as Delta Stepping, Dijkstra, and Bellman-Ford.
    """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the All Shortest Paths algorithm and returns a stream of results.

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
            Name of the relationship property to use as weights.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing 'sourceNodeId', 'targetNodeId', and 'distance' columns
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """Estimate memory requirements for the All Shortest Paths algorithm."""
        pass

    @property
    @abstractmethod
    def delta(self) -> SingleSourceDeltaEndpoints:
        """Access to Delta Stepping shortest path algorithm endpoints."""
        ...

    @property
    @abstractmethod
    def dijkstra(self) -> SingleSourceDijkstraEndpoints:
        """Access to Dijkstra shortest path algorithm endpoints."""
        ...
