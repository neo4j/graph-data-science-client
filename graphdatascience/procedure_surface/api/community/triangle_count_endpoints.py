from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class TriangleCountEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
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
    ) -> TriangleCountMutateResult:
        """
        Executes the Triangle Count algorithm and writes the results to the in-memory graph as node properties.

        The Triangle Count algorithm computes the number of triangles each node participates in.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress
            Display progress logging.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        TriangleCountMutateResult
            Algorithm metrics and statistics including the global triangle count and processing times
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
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
    ) -> TriangleCountStatsResult:
        """
        Executes the Triangle Count algorithm and returns statistics about the computation.

        This method computes triangle counts without storing results in the graph, providing
        aggregate statistics about the triangle structure of the graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress
            Display progress logging.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        TriangleCountStatsResult
            Algorithm statistics including the global triangle count and processing times
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
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
        Executes the Triangle Count algorithm and returns a stream of results.

        The Triangle Count algorithm computes the number of triangles each node participates in.
        This method returns the triangle count for each node as a stream.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress
            Display progress logging.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        DataFrame
            A DataFrame with columns:
            - nodeId: The node identifier
            - triangleCount: The number of triangles the node participates in
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
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
        write_concurrency: int | None = None,
    ) -> TriangleCountWriteResult:
        """
        Executes the Triangle Count algorithm and writes the results back to the database.

        This method computes triangle counts and writes the results directly to the Neo4j database
        as node properties, making them available for subsequent Cypher queries.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress
            Display progress logging.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        TriangleCountWriteResult
            Algorithm metrics and statistics including the global triangle count and processing times
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        label_filter: list[str] | None = None,
        max_degree: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        """
        Estimate the memory requirements for running the Triangle Count algorithm.

        This method provides memory estimates without actually running the algorithm, helping you
        determine if you have sufficient memory available.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to estimate for, or a graph configuration dictionary
        concurrency
            Number of concurrent threads to use.
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.

        Returns
        -------
        EstimationResult
            The memory estimation result including required memory in bytes and as heap percentage
        """


class TriangleCountMutateResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_triangle_count: int
    mutate_millis: int
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int


class TriangleCountStatsResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_triangle_count: int
    node_count: int
    post_processing_millis: int
    pre_processing_millis: int


class TriangleCountWriteResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_triangle_count: int
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    write_millis: int
