from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class PrizeSteinerTreeMutateResult(BaseResult):
    relationships_written: int
    mutate_millis: int
    effective_node_count: int
    sum_of_prizes: float
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class PrizeSteinerTreeWriteResult(BaseResult):
    relationships_written: int
    write_millis: int
    effective_node_count: int
    sum_of_prizes: float
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class PrizeSteinerTreeStatsResult(BaseResult):
    effective_node_count: int
    sum_of_prizes: float
    total_weight: float
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class PrizeSteinerTreeEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        prize_property: str,
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
        Runs the Prize Steiner tree algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G
           Graph object to use
        prize_property : str
            The name of the node property containing prize values.
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
            A DataFrame containing the tree edges with columns: nodeId, parentId, weight.
        """
        ...

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> PrizeSteinerTreeStatsResult:
        """
        Runs the Prize Steiner tree algorithm in stats mode, returning statistics without modifying the graph.

        Parameters
        ----------
        G
           Graph object to use
        prize_property : str
            The name of the node property containing prize values.
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
        PrizeSteinerTreeStatsResult
            Statistics about the computed Prize Steiner tree.
        """
        ...

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> PrizeSteinerTreeMutateResult:
        """
        Runs the Prize Steiner tree algorithm and adds the result as new relationships to the in-memory graph.

        Parameters
        ----------
        G
           Graph object to use
        mutate_relationship_type
            Name of the relationship type to store the results in.
        mutate_property
            Name of the node property to store the results in.
        prize_property : str
            The name of the node property containing prize values.
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
        PrizeSteinerTreeMutateResult
            Result containing statistics and timing information.
        """
        ...

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> PrizeSteinerTreeWriteResult:
        """
        Runs the Prize Steiner tree algorithm and writes the result back to the Neo4j database.

        Parameters
        ----------
        G
           Graph object to use
        write_relationship_type : str
            Name of the relationship type to store the results in.
        write_property
            Name of the node property to store the results in.
        prize_property : str
            The name of the node property containing prize values.
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
        PrizeSteinerTreeWriteResult
            Result containing statistics and timing information.
        """
        ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Prize Steiner tree algorithm.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        prize_property : str
            The name of the node property containing prize values.
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
            Memory estimation results including required bytes and percentages.
        """
        ...
