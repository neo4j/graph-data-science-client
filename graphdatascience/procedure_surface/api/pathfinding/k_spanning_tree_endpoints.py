from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES


class KSpanningTreeWriteResult(BaseResult):
    effective_node_count: int
    write_millis: int
    post_processing_millis: int
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class KSpanningTreeEndpoints(ABC):
    @abstractmethod
    def write(
        self,
        G: GraphV2,
        k: int,
        write_property: str,
        source_node: int,
        relationship_weight_property: str | None = None,
        objective: str = "minimum",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> KSpanningTreeWriteResult:
        """
        Runs the k-Spanning tree algorithm and writes the result back to the Neo4j database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        k : int
            The number of spanning trees to compute.
        write_property : str
            The property name to store the edge weight.
        source_node : int
            The source node (root) for the k-Spanning trees.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        objective : str, default="minimum"
            The objective function to optimize. Either "minimum" or "maximum".
        relationship_types : list[str], optional
            Filter to only use relationships of specific types.
        node_labels : list[str], optional
            Filter to only use nodes with specific labels.
        sudo : bool, default=False
            Whether to run with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id : str, optional
            An optional job ID for tracking the operation.
        write_concurrency : int, optional
            The number of threads to use for writing results.

        Returns
        -------
        KSpanningTreeWriteResult
            Result containing statistics and timing information.
        """
        ...
