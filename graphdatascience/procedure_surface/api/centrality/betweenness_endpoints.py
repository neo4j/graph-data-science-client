from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class BetweennessEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> BetweennessMutateResult:
        """
        Runs the Betweenness Centrality algorithm and stores the results in the graph catalog as a new node property.

        Betweenness centrality is a way of detecting the amount of influence a node has over the flow of information in a graph.
        It is often used to find nodes that serve as a bridge from one part of a graph to another.
        The algorithm calculates shortest paths between all pairs of nodes in a graph.
        Each node receives a score, based on the number of shortest paths that pass through the node.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            Name of the node property to store the results in.
        sampling_size : int | None, default=None
            Number of source nodes to consider for computing centrality scores.
        sampling_seed : int | None, default=None
            Seed value for the random number generator that selects source nodes.
        relationship_types : list[str] | None, default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str] | None, default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool | None, default=None
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.

        Returns
        -------
        BetweennessMutateResult
            Algorithm metrics and statistics including centrality distribution
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> BetweennessStatsResult:
        """
        Runs the Betweenness Centrality algorithm and returns result statistics without storing the results.

        Betweenness centrality is a way of detecting the amount of influence a node has over the flow of information in a graph.
        It is often used to find nodes that serve as a bridge from one part of a graph to another.
        The algorithm calculates shortest paths between all pairs of nodes in a graph.
        Each node receives a score, based on the number of shortest paths that pass through the node.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        sampling_size : int | None, default=None
            Number of source nodes to consider for computing centrality scores.
        sampling_seed : int | None, default=None
            Seed value for the random number generator that selects source nodes.
        relationship_types : list[str] | None, default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str] | None, default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool | None, default=None
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.

        Returns
        -------
        BetweennessStatsResult
            Algorithm statistics including centrality distribution
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> DataFrame:
        """
        Executes the Betweenness Centrality algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        sampling_size : int | None, default=None
            The number of nodes to use for sampling.
        sampling_seed : int | None, default=None
            The seed value for sampling randomization
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        relationship_weight_property : str | None, default=None
            The property name that contains relationship weights

        Returns
        -------
        DataFrame
            DataFrame with nodeId and score columns containing betweenness centrality results
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        write_concurrency: Any | None = None,
    ) -> BetweennessWriteResult:
        """
        Runs the Betweenness Centrality algorithm and stores the result in the Neo4j database as a new node property.

        Betweenness centrality is a way of detecting the amount of influence a node has over the flow of information in a graph.
        It is often used to find nodes that serve as a bridge from one part of a graph to another.
        The algorithm calculates shortest paths between all pairs of nodes in a graph.
        Each node receives a score, based on the number of shortest paths that pass through the node.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            Name of the node property to store the results in.
        sampling_size : int | None, default=None
            Number of source nodes to consider for computing centrality scores.
        sampling_seed : int | None, default=None
            Seed value for the random number generator that selects source nodes.
        relationship_types : list[str] | None, default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str] | None, default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool | None, default=None
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        write_concurrency : Any | None, default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        BetweennessWriteResult
            Algorithm metrics and statistics including centrality distribution
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        sampling_size : int | None, default=None
            The number of nodes to use for sampling.
        sampling_seed : int | None, default=None
            The seed value for sampling randomization
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads
        relationship_weight_property : str | None, default=None
            The property name that contains weight

        Returns
        -------
        EstimationResult
            Memory estimation details
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
