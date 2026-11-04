from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.k_spanning_tree_endpoints import (
    KSpanningTreeEndpoints,
)


class KSpanningTreeSessionEndpoints(KSpanningTreeEndpoints):
    """KSpanningTree endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        k: int,
        source_node: int,
        *,
        relationship_weight_property: str | None = None,
        objective: str = "minimum",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        k

        source_node

        relationship_weight_property

        objective

        relationship_types

        node_labels

        sudo

        log_progress

        username

        concurrency

        job_id


        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
