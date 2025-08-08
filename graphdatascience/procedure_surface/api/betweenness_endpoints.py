from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult

from ...graph.graph_object import Graph
from .estimation_result import EstimationResult


class BetweennessEndpoints(ABC):
    """
    Abstract base class defining the API for the Betweenness Centrality algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> BetweennessMutateResult:
        """
        Executes the Betweenness Centrality algorithm and returns result statistics without persisting the results

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the betweenness centrality score for each node
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights

        Returns
        -------
        BetweennessMutateResult
            Algorithm metrics and statistics including centrality distribution
        """

    @abstractmethod
    def stats(
        self,
        G: Graph,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> BetweennessStatsResult:
        """
        Executes the Betweenness Centrality algorithm and returns result statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights

        Returns
        -------
        BetweennessStatsResult
            Algorithm statistics including centrality distribution
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
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
        Executes the Betweenness Centrality algorithm and returns the results as a stream.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights

        Returns
        -------
        DataFrame
            DataFrame with nodeId and score columns containing betweenness centrality results
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        write_property: str,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
    ) -> BetweennessWriteResult:
        """
        Executes the Betweenness Centrality algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to store the betweenness centrality score for each node
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        BetweennessWriteResult
            Algorithm metrics and statistics including centrality distribution
        """

    @abstractmethod
    def estimate(self, G: Union[Graph, dict[str, Any]]) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """


class BetweennessMutateResult(BaseResult):
    """Result of running Betweenness Centrality algorithm with mutate mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]


class BetweennessStatsResult(BaseResult):
    """Result of running Betweenness Centrality algorithm with stats mode."""

    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class BetweennessWriteResult(BaseResult):
    """Result of running Betweenness Centrality algorithm with write mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]
