from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class SpanningTreeMutateResult(BaseResult):
    relationships_written: int
    mutate_millis: int
    effective_node_count: int
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SpanningTreeWriteResult(BaseResult):
    relationships_written: int
    write_millis: int
    effective_node_count: int
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SpanningTreeStatsResult(BaseResult):
    effective_node_count: int
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SpanningTreeEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
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
    ) -> DataFrame:
        """
        Runs the Spanning tree algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        source_node : int
            The source node (root) for the Spanning tree.
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

        Returns
        -------
        DataFrame
            A DataFrame containing the edges in the computed Spanning tree.
        """
        ...

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
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
    ) -> SpanningTreeStatsResult:
        """
        Runs the Spanning tree algorithm in stats mode, returning statistics without modifying the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        source_node : int
            The source node (root) for the Spanning tree.
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

        Returns
        -------
        SpanningTreeStatsResult
            Statistics about the computed Spanning tree.
        """
        ...

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
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
    ) -> SpanningTreeMutateResult:
        """
        Runs the Spanning tree algorithm and adds the result as new relationships to the in-memory graph.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property : str
            The property name to store the edge weight.
        source_node : int
            The source node (root) for the Spanning tree.
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

        Returns
        -------
        SpanningTreeMutateResult
            Result containing statistics and timing information.
        """
        ...

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
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
    ) -> SpanningTreeWriteResult:
        """
        Runs the Spanning tree algorithm and writes the result back to the Neo4j database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property : str
            The property name to store the edge weight.
        source_node : int
            The source node (root) for the Spanning tree.
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
        SpanningTreeWriteResult
            Result containing statistics and timing information.
        """
        ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        relationship_weight_property: str | None = None,
        objective: str = "minimum",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Spanning tree algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to estimate for, or a dictionary with nodeCount and relationshipCount.
        source_node : int
            The source node (root) for the Spanning tree.
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
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.

        Returns
        -------
        EstimationResult
            Memory estimation results including required bytes and percentages.
        """
        ...
