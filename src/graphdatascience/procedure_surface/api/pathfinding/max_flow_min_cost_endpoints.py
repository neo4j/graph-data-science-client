from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class MaxFlowMinCostEndpoints(ABC):
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
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> MaxFlowMinCostMutateResult:
        """
        Runs the Min-Cost Max Flow algorithm and stores the results in the graph catalog.

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
        cost_property
            Name of the relationship property containing costs.
        alpha
            Rate of cost-scaling in the refinement phase of the algorithm. Tuning can improve speed.
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
        MaxFlowMinCostMutateResult
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
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> MaxFlowMinCostStatsResult:
        """
        Runs the Min-Cost Max Flow algorithm and returns statistics.

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
        cost_property
            Name of the relationship property containing costs.
        alpha
            Rate of cost-scaling in the refinement phase of the algorithm. Tuning can improve speed.
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
        MaxFlowMinCostStatsResult
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
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Runs the Min-Cost Max Flow algorithm and streams the flows on relationships.

        Returns
        -------
        DataFrame
            Dataframe containing `source`, `target`, and `flow` per relationship.
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
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> MaxFlowMinCostWriteResult:
        """
        Runs the Min-Cost Max Flow algorithm and writes the results to the Neo4j database.

        Returns
        -------
        MaxFlowMinCostWriteResult
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
        cost_property: str | None = None,
        alpha: int = 6,
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.
        """
        pass


class MaxFlowMinCostMutateResult(BaseResult):
    total_flow: float
    total_cost: float
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class MaxFlowMinCostStatsResult(BaseResult):
    total_flow: float
    total_cost: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class MaxFlowMinCostWriteResult(BaseResult):
    total_flow: float
    total_cost: float
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    relationships_written: int
    configuration: dict[str, Any]
