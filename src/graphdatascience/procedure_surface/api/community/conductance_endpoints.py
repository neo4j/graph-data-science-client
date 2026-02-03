from __future__ import annotations

from abc import ABC, abstractmethod

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES


class ConductanceEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        community_property: str,
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
        Executes the Conductance algorithm and returns a stream of results.

        Parameters
        ----------
        G
            Graph object to use
        community_property
            Name of the node property containing community assignments.
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
        DataFrame
            DataFrame with the algorithm results containing 'community' and 'conductance' columns
        """
        pass
