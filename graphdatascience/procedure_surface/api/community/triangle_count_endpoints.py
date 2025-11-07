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
        sudo: bool | None = False,
        username: str | None = None,
    ) -> TriangleCountMutateResult:
        """
        Executes the Triangle Count algorithm and writes the results to the in-memory graph as node properties.

        The Triangle Count algorithm computes the number of triangles each node participates in.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the triangle count for each node
        concurrency : int | None, default=None
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        job_id : str | None, default=None
            An identifier for the job that can be used to cancel or monitor progress
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress : bool, default=True
            Whether to log progress information during execution
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        sudo : bool | None, default=False
            Override memory estimation limits. Setting this to True allows running the algorithm
            even if the estimated memory requirements exceed available memory.
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
        sudo: bool | None = False,
        username: str | None = None,
    ) -> TriangleCountStatsResult:
        """
        Executes the Triangle Count algorithm and returns statistics about the computation.

        This method computes triangle counts without storing results in the graph, providing
        aggregate statistics about the triangle structure of the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        job_id : str | None, default=None
            An identifier for the job that can be used to cancel or monitor progress
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress : bool, default=True
            Whether to log progress information during execution
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        sudo : bool | None, default=False
            Override memory estimation limits. Setting this to True allows running the algorithm
            even if the estimated memory requirements exceed available memory.
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
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Triangle Count algorithm and returns a stream of results.

        The Triangle Count algorithm computes the number of triangles each node participates in.
        This method returns the triangle count for each node as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        job_id : str | None, default=None
            An identifier for the job that can be used to cancel or monitor progress
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress : bool, default=True
            Whether to log progress information during execution
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        sudo : bool | None, default=False
            Override memory estimation limits. Setting this to True allows running the algorithm
            even if the estimated memory requirements exceed available memory.
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
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> TriangleCountWriteResult:
        """
        Executes the Triangle Count algorithm and writes the results back to the database.

        This method computes triangle counts and writes the results directly to the Neo4j database
        as node properties, making them available for subsequent Cypher queries.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the triangle count for each node in the database
        concurrency : int | None, default=None
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        job_id : str | None, default=None
            An identifier for the job that can be used to cancel or monitor progress
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        log_progress : bool, default=True
            Whether to log progress information during execution
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        sudo : bool | None, default=False
            Override memory estimation limits. Setting this to True allows running the algorithm
            even if the estimated memory requirements exceed available memory.
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads for writing results to the database

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
        concurrency : int | None, default=None
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        label_filter : list[str] | None, default=None
            Filter triangles by node labels. Only triangles where all nodes have one of the specified
            labels will be counted.
        max_degree : int | None, default=None
            Maximum degree of nodes to consider. Nodes with higher degrees will be excluded from
            triangle counting to improve performance.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.

        Returns
        -------
        EstimationResult
            The memory estimation result including required memory in bytes and as heap percentage
        """


class TriangleCountMutateResult(BaseResult):
    """
    Result object returned by the Triangle Count mutate operation.

    Attributes
    ----------
    compute_millis : int
        Time spent on computation in milliseconds
    configuration : dict[str, Any]
        The configuration used for the algorithm execution
    global_triangle_count : int
        The total number of triangles in the graph
    mutate_millis : int
        Time spent on mutating the graph in milliseconds
    node_count : int
        The total number of nodes processed
    node_properties_written : int
        The number of node properties written to the graph
    post_processing_millis : int
        Time spent on post-processing in milliseconds
    pre_processing_millis : int
        Time spent on pre-processing in milliseconds
    """

    compute_millis: int
    configuration: dict[str, Any]
    global_triangle_count: int
    mutate_millis: int
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int


class TriangleCountStatsResult(BaseResult):
    """
    Result object returned by the Triangle Count stats operation.

    Attributes
    ----------
    compute_millis : int
        Time spent on computation in milliseconds
    configuration : dict[str, Any]
        The configuration used for the algorithm execution
    global_triangle_count : int
        The total number of triangles in the graph
    node_count : int
        The total number of nodes processed
    post_processing_millis : int
        Time spent on post-processing in milliseconds
    pre_processing_millis : int
        Time spent on pre-processing in milliseconds
    """

    compute_millis: int
    configuration: dict[str, Any]
    global_triangle_count: int
    node_count: int
    post_processing_millis: int
    pre_processing_millis: int


class TriangleCountWriteResult(BaseResult):
    """
    Result object returned by the Triangle Count write operation.

    Attributes
    ----------
    compute_millis : int
        Time spent on computation in milliseconds
    configuration : dict[str, Any]
        The configuration used for the algorithm execution
    global_triangle_count : int
        The total number of triangles in the graph
    node_count : int
        The total number of nodes processed
    node_properties_written : int
        The number of node properties written to the database
    post_processing_millis : int
        Time spent on post-processing in milliseconds
    pre_processing_millis : int
        Time spent on pre-processing in milliseconds
    write_millis : int
        Time spent on writing results to the database in milliseconds
    """

    compute_millis: int
    configuration: dict[str, Any]
    global_triangle_count: int
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    write_millis: int
