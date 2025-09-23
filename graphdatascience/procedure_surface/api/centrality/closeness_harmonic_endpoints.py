from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ClosenessHarmonicEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessHarmonicMutateResult:
        """
        Runs the Harmonic Centrality algorithm and stores the results in the graph catalog as a new node property.

        Harmonic centrality (also known as valued centrality) is a variant of closeness centrality, that was invented to
        solve the problem the original formula had when dealing with unconnected graphs.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the harmonic closeness centrality score for each node
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
        ClosenessHarmonicMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ClosenessHarmonicStatsResult:
        """
        Runs the Harmonic Centrality algorithm and returns result statistics without storing the results.

        Harmonic centrality was proposed by Marchiori and Latora while trying to come up with a sensible notion of "average shortest path".
        They suggested a different way of calculating the average distance to that used in the Closeness Centrality algorithm.
        Rather than summing the distances of a node to all other nodes, the harmonic centrality algorithm sums the inverse of those distances.
        This enables it deal with infinite values.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
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
        ClosenessHarmonicStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the Harmonic Closeness Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
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
            DataFrame with the algorithm results containing nodeId and score columns
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> ClosenessHarmonicWriteResult:
        """
        Runs the Harmonic Centrality algorithm and stores the result in the Neo4j database as a new node property.

        Harmonic centrality was proposed by Marchiori and Latora while trying to come up with a sensible notion of "average shortest path".
        They suggested a different way of calculating the average distance to that used in the Closeness Centrality algorithm.
        Rather than summing the distances of a node to all other nodes, the harmonic centrality algorithm sums the inverse of those distances.
        This enables it deal with infinite values.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the harmonic closeness centrality scores to
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
        G: Union[GraphV2, dict[str, Any]],
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of a Harmonic Closeness Centrality algorithm run.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
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
