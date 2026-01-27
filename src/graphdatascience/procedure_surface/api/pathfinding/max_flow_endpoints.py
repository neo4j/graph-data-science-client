from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class MaxFlowEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        mutate_property: str,
        mutate_relationship_type: str,
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> MaxFlowMutateResult:
        """
        Runs the Max Flow algorithm and stores the results in the graph catalog.

        Parameters
        ----------
        G
           Graph object to use
        source_nodes
            List of source node IDs.
        target_nodes
            List of target node IDs.
        mutate_property
            Name of the node property to store the results in.
        mutate_relationship_type
            Name of the relationship type to store the results in.
        capacity_property
            Name of the relationship property containing capacities.
        node_capacity_property
            Name of the node property containing capacities.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.

        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        MaxFlowMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> MaxFlowStatsResult:
        """
        Runs the Max Flow algorithm and returns statistics.

        Parameters
        ----------
        G
           Graph object to use
        source_nodes
            List of source node IDs.
        target_nodes
            List of target node IDs.
        capacity_property
            Name of the relationship property containing capacities.
        node_capacity_property
            Name of the node property containing capacities.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        MaxFlowStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Runs the Max Flow algorithm and returns a stream of results.

        Parameters
        ----------
        G
           Graph object to use
        source_nodes
            List of source node IDs.
        target_nodes
            List of target node IDs.
        capacity_property
            Name of the relationship property containing capacities.
        node_capacity_property
            Name of the node property containing capacities.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing 'source', 'target', and 'flow' columns
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        source_nodes: list[int],
        target_nodes: list[int],
        write_property: str,
        write_relationship_type: str,
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> MaxFlowWriteResult:
        """
        Runs the Max Flow algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G
           Graph object to use
        source_nodes
            List of source node IDs.
        target_nodes
            List of target node IDs.
        write_property
            Name of the node property to store the results in.
        write_relationship_type
            Name of the relationship type to store the results in.
        capacity_property
            Name of the relationship property containing capacities.
        node_capacity_property
            Name of the node property containing capacities.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        MaxFlowWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_nodes: list[int],
        target_nodes: list[int],
        *,
        capacity_property: str | None = None,
        node_capacity_property: str | None = None,
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        source_nodes
            List of source node IDs.
        target_nodes
            List of target node IDs.
        capacity_property
            Name of the relationship property containing capacities.
        node_capacity_property
            Name of the node property containing capacities.
        concurrency
            Number of concurrent threads to use.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class MaxFlowMutateResult(BaseResult):
    total_flow: float
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class MaxFlowStatsResult(BaseResult):
    total_flow: float
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class MaxFlowWriteResult(BaseResult):
    total_flow: float
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    relationships_written: int
    configuration: dict[str, Any]
