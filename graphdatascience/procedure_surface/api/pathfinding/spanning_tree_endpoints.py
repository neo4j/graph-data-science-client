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
        G
            The graph to run the algorithm on.
        source_node : int
            The source node (root) for the Spanning tree.
        relationship_weight_property
            Name of the property to be used as weights.
        objective : str, default="minimum"
            The objective function to optimize. Either "minimum" or "maximum".
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
        G
            The graph to run the algorithm on.
        source_node : int
            The source node (root) for the Spanning tree.
        relationship_weight_property
            Name of the property to be used as weights.
        objective : str, default="minimum"
            The objective function to optimize. Either "minimum" or "maximum".
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
        G
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property
            Name of the node property to store the results in.
        source_node : int
            The source node (root) for the Spanning tree.
        relationship_weight_property
            Name of the property to be used as weights.
        objective : str, default="minimum"
            The objective function to optimize. Either "minimum" or "maximum".
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
        G
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property
            Name of the node property to store the results in.
        source_node : int
            The source node (root) for the Spanning tree.
        relationship_weight_property
            Name of the property to be used as weights.
        objective : str, default="minimum"
            The objective function to optimize. Either "minimum" or "maximum".
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
        write_concurrency
            Number of concurrent threads to use for writing.Returns
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
        relationship_weight_property
            Name of the property to be used as weights.
        objective : str, default="minimum"
            The objective function to optimize. Either "minimum" or "maximum".
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.

        Returns
        -------
        EstimationResult
            Memory estimation results including required bytes and percentages.
        """
        ...
