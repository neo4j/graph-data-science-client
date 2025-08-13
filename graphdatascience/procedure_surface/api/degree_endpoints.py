from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...graph.graph_object import Graph
from .base_result import BaseResult
from .estimation_result import EstimationResult


class DegreeEndpoints(ABC):
    """
    Abstract base class defining the API for the Degree Centrality algorithm.

    Degree centrality measures the number of incoming and outgoing relationships from a node.
    It's one of the simplest centrality measures, where a node's importance is determined by
    the number of direct connections it has.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DegreeMutateResult:
        """
        Executes the Degree Centrality algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the degree centrality score for each node
        orientation : Optional[Any], default=None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
            'NATURAL' (default) respects the direction of relationships as they are stored in the graph.
            'REVERSE' treats each relationship as if it were directed in the opposite direction.
            'UNDIRECTED' treats all relationships as undirected, effectively counting both directions.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights. If specified,
            weighted degree centrality is computed where each relationship contributes
            its weight to the total degree.

        Returns
        -------
        DegreeMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DegreeStatsResult:
        """
        Executes the Degree Centrality algorithm and returns statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        orientation : Optional[Any], default=None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
            'NATURAL' (default) respects the direction of relationships as they are stored in the graph.
            'REVERSE' treats each relationship as if it were directed in the opposite direction.
            'UNDIRECTED' treats all relationships as undirected, effectively counting both directions.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights. If specified,
            weighted degree centrality is computed where each relationship contributes
            its weight to the total degree.

        Returns
        -------
        DegreeStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the Degree Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        orientation : Optional[Any], default=None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
            'NATURAL' (default) respects the direction of relationships as they are stored in the graph.
            'REVERSE' treats each relationship as if it were directed in the opposite direction.
            'UNDIRECTED' treats all relationships as undirected, effectively counting both directions.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation
        relationship_weight_property : Optional[str], default=None
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
        G: Graph,
        write_property: str,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
    ) -> DegreeWriteResult:
        """
        Executes the Degree Centrality algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to store the degree centrality score for each node in the database
        orientation : Optional[Any], default=None
            The orientation of relationships to consider. Can be 'NATURAL', 'REVERSE', or 'UNDIRECTED'.
            'NATURAL' (default) respects the direction of relationships as they are stored in the graph.
            'REVERSE' treats each relationship as if it were directed in the opposite direction.
            'UNDIRECTED' treats all relationships as undirected, effectively counting both directions.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights. If specified,
            weighted degree centrality is computed where each relationship contributes
            its weight to the total degree.
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads used during the write phase.

        Returns
        -------
        DegreeWriteResult
            Algorithm metrics and statistics including the centrality distribution and write timing
        """
        pass

    @abstractmethod
    def estimate(self, G: Union[Graph, dict[str, Any]]) -> EstimationResult:
        """
        Estimate the memory consumption of the Degree Centrality algorithm.

        This method provides an estimate of the memory requirements for running the algorithm
        on a given graph, helping with capacity planning and resource allocation.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation including memory requirements
        """
        pass


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
