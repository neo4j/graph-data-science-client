from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class DijkstraStreamResult(BaseResult):
    index: int
    source_node: int
    target_node: int
    total_cost: float
    node_ids: list[int]
    costs: list[float]
    path: list[int]


class DijkstraWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class DijkstraMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class DijkstraEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: int | list[int],
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the Dijkstra shortest path algorithm and returns the result as a DataFrame.

        The Dijkstra algorithm computes the shortest path(s) from a source node to one or more target nodes
        in a weighted graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        source_node : int
            The source node for the shortest path computation.
        target_nodes : int | list[int]
            A single target node or a list of target nodes for the shortest path computation.
        relationship_weight_property
            Name of the property to be used as weights.
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
            The shortest path results as a DataFrame with columns for sourceNode, targetNode, totalCost, nodeIds, costs, index.
        """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        source_node: int,
        target_nodes: int | list[int],
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DijkstraMutateResult:
        """
        Runs the Dijkstra shortest path algorithm and stores the results as new relationships in the graph catalog.

        The Dijkstra algorithm computes the shortest path(s) from a source node to one or more target nodes
        in a weighted graph and creates relationships representing the paths in the in-memory graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships in the graph catalog.
        source_node : int
            The source node for the shortest path computation.
        target_nodes : int | list[int]
            A single target node or a list of target nodes for the shortest path computation.
        relationship_weight_property
            Name of the property to be used as weights.
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
        DijkstraMutateResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        source_node: int,
        target_nodes: int | list[int],
        write_node_ids: bool = False,
        write_costs: bool = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> DijkstraWriteResult:
        """
        Runs the Dijkstra shortest path algorithm and writes the results back to the database.

        The Dijkstra algorithm computes the shortest path(s) from a source node to one or more target nodes
        in a weighted graph and writes the path as relationships.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        source_node : int
            The source node for the shortest path computation.
        target_nodes : int | list[int]
            A single target node or a list of target nodes for the shortest path computation.
        write_node_ids : bool, default=False
            Whether to write node IDs along the path.
        write_costs : bool, default=False
            Whether to write costs along the path.
        relationship_weight_property
            Name of the property to be used as weights.
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
        write_concurrency
            Number of concurrent threads to use for writing.Returns
        -------
        DijkstraWriteResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_nodes: int | list[int],
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Dijkstra shortest path algorithm.

        The Dijkstra algorithm computes the shortest path(s) from a source node to one or more target nodes
        in a weighted graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on or a dictionary representing the graph dimensions.
        source_node : int
            The source node for the shortest path computation.
        target_nodes : int | list[int]
            A single target node or a list of target nodes for the shortest path computation.
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.

        Returns
        -------
        EstimationResult
            Object containing the estimated memory requirements.
        """
