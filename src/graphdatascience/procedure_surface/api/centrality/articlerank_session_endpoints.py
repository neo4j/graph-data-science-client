from __future__ import annotations

from abc import abstractmethod

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.centrality.articlerank_endpoints import ArticleRankEndpoints
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class ArticleRankSessionEndpoints(ArticleRankEndpoints):
    """Article Rank endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        damping_factor
            The damping factor controls the probability of a random jump to a random node
        tolerance
            Minimum change in scores between iterations.
        max_iterations
            Maximum number of iterations to run.
        scaler
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
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
        relationship_weight_property
            Name of the property to be used as weights.
        source_nodes
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
