from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class SteinerTreeMutateResult(BaseResult):
    relationships_written: int
    mutate_millis: int
    effective_node_count: int
    effective_target_nodes_count: int
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SteinerTreeWriteResult(BaseResult):
    relationships_written: int
    write_millis: int
    effective_node_count: int
    effective_target_nodes_count: int
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SteinerTreeStatsResult(BaseResult):
    effective_node_count: int
    effective_target_nodes_count: int
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SteinerTreeEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the Steiner tree algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        source_node : int
            The source node (root) for the Steiner tree.
        target_nodes : list[int]
            The list of target nodes (terminals) that must be connected.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        delta : float, default=2.0
            The delta parameter for the shortest path computation used internally.
        apply_rerouting : bool, default=False
            Whether to apply rerouting optimization to improve the tree.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id
            Identifier for the computation.

        Returns
        -------
        DataFrame
            A DataFrame containing the edges in the computed Steiner tree.
        """
        ...

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> SteinerTreeStatsResult:
        """
        Runs the Steiner tree algorithm in stats mode, returning statistics without modifying the graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        source_node : int
            The source node (root) for the Steiner tree.
        target_nodes : list[int]
            The list of target nodes (terminals) that must be connected.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        delta : float, default=2.0
            The delta parameter for the shortest path computation used internally.
        apply_rerouting : bool, default=False
            Whether to apply rerouting optimization to improve the tree.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id
            Identifier for the computation.

        Returns
        -------
        SteinerTreeStatsResult
            Statistics about the computed Steiner tree.
        """
        ...

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> SteinerTreeMutateResult:
        """
        Runs the Steiner tree algorithm and adds the result as new relationships to the in-memory graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property : str
            The property name to store the edge weight.
        source_node : int
            The source node (root) for the Steiner tree.
        target_nodes : list[int]
            The list of target nodes (terminals) that must be connected.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        delta : float, default=2.0
            The delta parameter for the shortest path computation used internally.
        apply_rerouting : bool, default=False
            Whether to apply rerouting optimization to improve the tree.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id
            Identifier for the computation.

        Returns
        -------
        SteinerTreeMutateResult
            Result containing statistics and timing information.
        """
        ...

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> SteinerTreeWriteResult:
        """
        Runs the Steiner tree algorithm and writes the result back to the Neo4j database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property : str
            The property name to store the edge weight.
        source_node : int
            The source node (root) for the Steiner tree.
        target_nodes : list[int]
            The list of target nodes (terminals) that must be connected.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        delta : float, default=2.0
            The delta parameter for the shortest path computation used internally.
        apply_rerouting : bool, default=False
            Whether to apply rerouting optimization to improve the tree.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id
            Identifier for the computation.
        write_concurrency : int, optional
            The number of threads to use for writing results.

        Returns
        -------
        SteinerTreeWriteResult
            Result containing statistics and timing information.
        """
        ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Steiner tree algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to estimate for, or a dictionary with nodeCount and relationshipCount.
        source_node : int
            The source node (root) for the Steiner tree.
        target_nodes : list[int]
            The list of target nodes (terminals) that must be connected.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        delta : float, default=2.0
            The delta parameter for the shortest path computation.
        apply_rerouting : bool, default=False
            Whether to apply rerouting optimization.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.

        Returns
        -------
        EstimationResult
            Memory estimation results including required bytes and percentages.
        """
        ...
