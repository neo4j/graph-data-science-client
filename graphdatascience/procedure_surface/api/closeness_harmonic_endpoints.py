from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...graph.graph_object import Graph
from .base_result import BaseResult
from .estimation_result import EstimationResult


class ClosenessHarmonicEndpoints(ABC):
    """
    Abstract base class defining the API for the Harmonic Centrality algorithm.

    Harmonic centrality is a variant of closeness centrality that uses the
    harmonic mean of shortest path distances. It handles disconnected graphs better
    than standard closeness centrality.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessHarmonicMutateResult:
        """
        Executes the Harmonic Closeness Centrality algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the harmonic closeness centrality score for each node
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
            If not specified, all relationship types are considered.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
            If not specified, all node labels are considered.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
            If not specified, uses the default concurrency level.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        ClosenessHarmonicMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessHarmonicStatsResult:
        """
        Executes the Harmonic Closeness Centrality algorithm and returns statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
            If not specified, all relationship types are considered.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
            If not specified, all node labels are considered.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
            If not specified, uses the default concurrency level.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        ClosenessHarmonicStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the Harmonic Closeness Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
            If not specified, all relationship types are considered.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
            If not specified, all node labels are considered.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
            If not specified, uses the default concurrency level.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing nodeId and score columns
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> ClosenessHarmonicWriteResult:
        """
        Executes the Harmonic Closeness Centrality algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write the harmonic closeness centrality scores to
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
            If not specified, all relationship types are considered.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
            If not specified, all node labels are considered.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
            If not specified, uses the default concurrency level.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        ClosenessHarmonicWriteResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of a Harmonic Closeness Centrality algorithm run.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class ClosenessHarmonicMutateResult(BaseResult):
    """Result of running Harmonic Closeness Centrality algorithm with mutate mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]


class ClosenessHarmonicStatsResult(BaseResult):
    """Result of running Harmonic Closeness Centrality algorithm with stats mode."""

    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class ClosenessHarmonicWriteResult(BaseResult):
    """Result of running Harmonic Closeness Centrality algorithm with write mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]
