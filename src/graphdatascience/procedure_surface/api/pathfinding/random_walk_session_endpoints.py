from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.random_walk_endpoints import (
    RandomWalkEndpoints,
)


class RandomWalkSessionEndpoints(RandomWalkEndpoints):
    """RandomWalk endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
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
        source_nodes
            Nodes to start the random walks from. If not specified, walks start from every node.
        walk_length
            The length of each random walk.
        walks_per_node
            The number of walks to sample for each node.
        in_out_factor
            Controls the likelihood of immediately revisiting a node in the walk.
        return_factor
            Controls the likelihood of visiting already visited nodes.
        walk_buffer_size
            Buffer size for walk sampling.
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.
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
