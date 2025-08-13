from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...graph.graph_object import Graph
from .base_result import BaseResult
from .estimation_result import EstimationResult


class ClosenessEndpoints(ABC):
    """
    Abstract base class defining the API for the Closeness Centrality algorithm.

    Closeness centrality measures how close a node is to all other nodes in the network.
    It's calculated as the reciprocal of the sum of the shortest path distances from
    the node to all other nodes in the graph.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessMutateResult:
        """
        Executes the Closeness Centrality algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the closeness centrality score for each node
        use_wasserman_faust : Optional[bool], default=None
            Use the improved Wasserman-Faust formula for closeness computation.
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

        Returns
        -------
        ClosenessMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessStatsResult:
        """
        Executes the Closeness Centrality algorithm and returns statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        use_wasserman_faust : Optional[bool], default=None
            Use the improved Wasserman-Faust formula for closeness computation.
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

        Returns
        -------
        ClosenessStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the Closeness Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        use_wasserman_faust : Optional[bool], default=None
            Use the improved Wasserman-Faust formula for closeness computation.
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

        Returns
        -------
        DataFrame
            DataFrame with nodeId and score columns containing closeness centrality results.
            Each row represents a node with its corresponding closeness centrality score.
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        write_property: str,
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> ClosenessWriteResult:
        """
        Executes the Closeness Centrality algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write closeness centrality scores to in the Neo4j database
        use_wasserman_faust : Optional[bool], default=None
            Use the improved Wasserman-Faust formula for closeness computation.
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
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads used during the write phase.

        Returns
        -------
        ClosenessWriteResult
            Algorithm metrics and statistics including the number of properties written
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        use_wasserman_faust: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph configuration.
        use_wasserman_faust : Optional[bool], default=None
            Use the improved Wasserman-Faust formula for closeness computation.
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


class ClosenessMutateResult(BaseResult):
    """Result of running Closeness Centrality algorithm with mutate mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]


class ClosenessStatsResult(BaseResult):
    """Result of running Closeness Centrality algorithm with stats mode."""

    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class ClosenessWriteResult(BaseResult):
    """Result of running Closeness Centrality algorithm with write mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]
