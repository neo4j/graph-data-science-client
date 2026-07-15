from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.pathfinding.longest_path_endpoints import LongestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.topological_sort_endpoints import TopologicalSortEndpoints


class DagEndpoints(ABC):
    """
    Container for Directed Acyclic Graph (DAG) algorithm endpoints.
    """

    @property
    @abstractmethod
    def longest_path(self) -> LongestPathEndpoints:
        """Access to Longest Path algorithm endpoints for DAGs."""
        ...

    @property
    @abstractmethod
    def topological_sort(self) -> TopologicalSortEndpoints:
        """Access to Topological Sort algorithm endpoints for DAGs."""
        ...
