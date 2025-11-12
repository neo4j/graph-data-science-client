from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class YensWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class YensMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class SourceTargetYensEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_node: int,
        k: int,
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
        Runs the Yen's K shortest paths algorithm and returns the result as a DataFrame.

        The Yen's algorithm computes the K shortest paths from a source node to a target node.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        k : int
            The number of shortest paths to compute.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

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
        target_node: int,
        k: int,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> YensMutateResult:
        """
        Runs the Yen's K shortest paths algorithm and stores the results as new relationships in the graph catalog.

        The Yen's algorithm computes the K shortest paths from a source node to a target node.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships in the graph catalog.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        k : int
            The number of shortest paths to compute.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        YensMutateResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        source_node: int,
        target_node: int,
        k: int,
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
    ) -> YensWriteResult:
        """
        Runs the Yen's K shortest paths algorithm and writes the results back to the database.

        The Yen's algorithm computes the K shortest paths from a source node to a target node.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        k : int
            The number of shortest paths to compute.
        write_node_ids : bool, default=False
            Whether to write node IDs of the shortest path onto the relationship.
        write_costs : bool, default=False
            Whether to write costs of the shortest path onto the relationship.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.
        write_concurrency : int | None, default=None
            Concurrency for writing results.

        Returns
        -------
        YensWriteResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_node: int,
        k: int,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Yen's K shortest paths algorithm.

        The Yen's algorithm computes the K shortest paths from a source node to a target node.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        k : int
            The number of shortest paths to compute.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.

        Returns
        -------
        EstimationResult
            Object containing the estimated memory requirements.
        """
