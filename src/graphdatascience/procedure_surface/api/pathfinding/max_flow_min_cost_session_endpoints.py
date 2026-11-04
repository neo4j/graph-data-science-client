from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.max_flow_min_cost_endpoints import (
    MaxFlowMinCostEndpoints,
)


class MaxFlowMinCostSessionEndpoints(MaxFlowMinCostEndpoints):
    """MaxFlowMinCost endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
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
        source_nodes
            List of source node IDs.
        target_nodes
            List of target node IDs.
        capacity_property
            Name of the relationship property containing capacities.
        node_capacity_property
            Name of the node property containing capacities.
        cost_property
            Name of the relationship property containing costs.
        alpha
            Rate of cost-scaling in the refinement phase of the algorithm. Tuning can improve speed.
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
