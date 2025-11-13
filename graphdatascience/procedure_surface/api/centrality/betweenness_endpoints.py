from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class BetweennessEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        sampling_size: int | None = None,
        sampling_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
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
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        sampling_size : int | None, default=None
            Number of source nodes to consider for computing centrality scores.
        sampling_seed : int | None, default=None
            Seed value for the random number generator that selects source nodes.
        relationship_types : list[str]
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
        relationship_weight_property
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
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
        G
            The graph to run the algorithm on.
        sampling_size : int | None, default=None
            Number of source nodes to consider for computing centrality scores.
        sampling_seed : int | None, default=None
            Seed value for the random number generator that selects source nodes.
        relationship_types : list[str]
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
        relationship_weight_property
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
        Executes the Betweenness Centrality algorithm and returns the results as a stream.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        sampling_size : int | None, default=None
            The number of nodes to use for sampling.
        sampling_seed : int | None, default=None
            The seed value for sampling randomization
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
        relationship_weight_property
            Name of the property to be used as weights.

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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        write_concurrency: int | None = None,
    ) -> BetweennessWriteResult:
        """
        Runs the Betweenness Centrality algorithm and stores the result in the Neo4j database as a new node property.

        Betweenness centrality is a way of detecting the amount of influence a node has over the flow of information in a graph.
        It is often used to find nodes that serve as a bridge from one part of a graph to another.
        The algorithm calculates shortest paths between all pairs of nodes in a graph.
        Each node receives a score, based on the number of shortest paths that pass through the node.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        sampling_size : int | None, default=None
            Number of source nodes to consider for computing centrality scores.
        sampling_seed : int | None, default=None
            Seed value for the random number generator that selects source nodes.
        relationship_types : list[str]
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
        relationship_weight_property
            Name of the property to be used as weights.
        write_concurrency
            Number of concurrent threads to use for writing.Returns
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
        sampling_size : int | None, default=None
            The number of nodes to use for sampling.
        sampling_seed : int | None, default=None
            The seed value for sampling randomization
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        relationship_weight_property
            Name of the property to be used as weights.

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
