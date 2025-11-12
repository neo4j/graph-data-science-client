from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class DegreeEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        orientation: str = "NATURAL",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
    ) -> DegreeMutateResult:
        """
        Runs the Degree Centrality algorithm and stores the results in the graph catalog as a new node property.

        The Degree Centrality algorithm can be used to find popular nodes within a graph.
        The degree centrality measures the number of incoming or outgoing (or both) relationships from a node, which can be defined by the orientation of a relationship projection.
        It can be applied to either weighted or unweighted graphs.
        In the weighted case the algorithm computes the sum of all positive weights of adjacent relationships of a node, for each node in the graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            Name of the node property to store the results in.
        orientation : str | None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool, default=True
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            Number of threads to use for running the algorithm.
        job_id : str | None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.

        Returns
        -------
        DegreeMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        orientation: str = "NATURAL",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
    ) -> DegreeStatsResult:
        """
        Runs the Degree Centrality algorithm and returns result statistics without storing the results.

        The Degree Centrality algorithm can be used to find popular nodes within a graph.
        The degree centrality measures the number of incoming or outgoing (or both) relationships from a node, which can be defined by the orientation of a relationship projection.
        It can be applied to either weighted or unweighted graphs.
        In the weighted case the algorithm computes the sum of all positive weights of adjacent relationships of a node, for each node in the graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        orientation : str | None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool, default=True
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            Number of threads to use for running the algorithm.
        job_id : str | None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.

        Returns
        -------
        DegreeStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        orientation: str = "NATURAL",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
    ) -> DataFrame:
        """
        Executes the Degree Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        orientation : str | None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
            'NATURAL' (default) respects the direction of relationships as they are stored in the graph.
            'REVERSE' treats each relationship as if it were directed in the opposite direction.
            'UNDIRECTED' treats all relationships as undirected, effectively counting both directions.
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        sudo : bool
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation
        relationship_weight_property : str | None, default=None
            The property name that contains relationship weights. If specified,
            weighted degree centrality is computed where each relationship contributes
            its weight to the total degree.

        Returns
        -------
        DataFrame
            DataFrame with nodeId and score columns containing degree centrality results.
            Each row represents a node with its corresponding degree centrality score.
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        orientation: str = "NATURAL",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        write_concurrency: int | None = None,
    ) -> DegreeWriteResult:
        """
        Runs the Degree Centrality algorithm and stores the result in the Neo4j database as a new node property.

        The Degree Centrality algorithm can be used to find popular nodes within a graph.
        The degree centrality measures the number of incoming or outgoing (or both) relationships from a node, which can be defined by the orientation of a relationship projection.
        It can be applied to either weighted or unweighted graphs.
        In the weighted case the algorithm computes the sum of all positive weights of adjacent relationships of a node, for each node in the graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The property name to store the degree centrality score for each node in the database
        orientation : str | None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool, default=True
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of threads to use for running the algorithm.
        job_id : str | None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        write_concurrency : int | None
            The number of concurrent threads used during the write phase.

        Returns
        -------
        DegreeWriteResult
            Algorithm metrics and statistics including the centrality distribution and write timing
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        orientation: str = "NATURAL",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
            The graph to run the algorithm on or a dictionary representing the graph.
        orientation : str | None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        concurrency : int | None
            The number of concurrent threads
        relationship_weight_property : str | None, default=None
            The property name that contains weight

        Returns
        -------
        EstimationResult
            Memory estimation details
        """


class DegreeMutateResult(BaseResult):
    """Result of running Degree Centrality algorithm with mutate mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]


class DegreeStatsResult(BaseResult):
    """Result of running Degree Centrality algorithm with stats mode."""

    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class DegreeWriteResult(BaseResult):
    """Result of running Degree Centrality algorithm with write mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]
