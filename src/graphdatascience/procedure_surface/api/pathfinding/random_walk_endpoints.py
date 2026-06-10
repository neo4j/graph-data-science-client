from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class RandomWalkEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: Graph,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the Random Walk algorithm and returns a stream of results.

        Parameters
        ----------
        G
            Graph object to use
        source_nodes
            Nodes to start the random walks from. If not specified, walks start from every node.
        walk_length
            The length of each random walk.
        walks_per_node
            The number of walks to sample for each node.
        in_out_factor
            Controls the likelihood of immediately revisiting a node in the walk.
        return_factor
            Controls the likelihood of visiting already visited nodes.
        walk_buffer_size
            Buffer size for walk sampling.
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.
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

        Returns
        -------
        DataFrame
            DataFrame with nodeIds and path columns.
        """
        pass

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> RandomWalkMutateResult:
        """
        Runs the Random Walk algorithm and stores the results as a new node property in the graph catalog.

        Parameters
        ----------
        G
            Graph object to use
        mutate_property
            Name of the node property to store the results in.
        source_nodes
            Nodes to start the random walks from. If not specified, walks start from every node.
        walk_length
            The length of each random walk.
        walks_per_node
            The number of walks to sample for each node.
        in_out_factor
            Controls the likelihood of immediately revisiting a node in the walk.
        return_factor
            Controls the likelihood of visiting already visited nodes.
        walk_buffer_size
            Buffer size for walk sampling.
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.
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

        Returns
        -------
        RandomWalkMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> RandomWalkStatsResult:
        """
        Runs the Random Walk algorithm and returns result statistics without storing the results.

        Parameters
        ----------
        G
            Graph object to use
        source_nodes
            Nodes to start the random walks from. If not specified, walks start from every node.
        walk_length
            The length of each random walk.
        walks_per_node
            The number of walks to sample for each node.
        in_out_factor
            Controls the likelihood of immediately revisiting a node in the walk.
        return_factor
            Controls the likelihood of visiting already visited nodes.
        walk_buffer_size
            Buffer size for walk sampling.
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.
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

        Returns
        -------
        RandomWalkStatsResult
            Algorithm statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Graph | dict[str, Any],
        source_nodes: int | list[int] | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
            Graph object to use or a dictionary representing the graph dimensions.
        source_nodes
            Nodes to start the random walks from. If not specified, walks start from every node.
        walk_length
            The length of each random walk.
        walks_per_node
            The number of walks to sample for each node.
        in_out_factor
            Controls the likelihood of immediately revisiting a node in the walk.
        return_factor
            Controls the likelihood of visiting already visited nodes.
        walk_buffer_size
            Buffer size for walk sampling.
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class RandomWalkMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class RandomWalkStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]
