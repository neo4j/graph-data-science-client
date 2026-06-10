from __future__ import annotations

from abc import ABC, abstractmethod

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES


class TrianglesEndpoints(ABC):
    @abstractmethod
    def __call__(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        label_filter: list[str] | None = None,
        log_progress: bool = True,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Streams all triangles found in the graph.

        Parameters
        ----------
        G
           Graph object to use
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        label_filter : list[str] | None, default=None
            A list of up to three node labels as strings. Only triangles with nodes having representatives for each specified label will be counted.
        log_progress
            Display progress logging.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        DataFrame
            A DataFrame with columns:
            - nodeA: The first node identifier in the triangle
            - nodeB: The second node identifier in the triangle
            - nodeC: The third node identifier in the triangle
        """
        pass
