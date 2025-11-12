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
            The graph to run the algorithm on.
        prize_property : str
            The name of the node property containing prize values.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        relationship_types : list[str], optional
            Filter to only use relationships of specific types.
        node_labels : list[str], optional
            Filter to only use nodes with specific labels.
        sudo : bool, default=False
            Whether to run with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id : str, optional
            An optional job ID for tracking the operation.

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
            The graph to run the algorithm on.
        prize_property : str
            The name of the node property containing prize values.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        relationship_types : list[str], optional
            Filter to only use relationships of specific types.
        node_labels : list[str], optional
            Filter to only use nodes with specific labels.
        sudo : bool, default=False
            Whether to run with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id : str, optional
            An optional job ID for tracking the operation.

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
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property : str
            The property name to store the edge weight.
        prize_property : str
            The name of the node property containing prize values.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        relationship_types : list[str], optional
            Filter to only use relationships of specific types.
        node_labels : list[str], optional
            Filter to only use nodes with specific labels.
        sudo : bool, default=False
            Whether to run with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id : str, optional
            An optional job ID for tracking the operation.

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
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property : str
            The property name to store the edge weight.
        prize_property : str
            The name of the node property containing prize values.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        relationship_types : list[str], optional
            Filter to only use relationships of specific types.
        node_labels : list[str], optional
            Filter to only use nodes with specific labels.
        sudo : bool, default=False
            Whether to run with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress during execution.
        username : str, optional
            The username to use for logging.
        concurrency : int, optional
            The number of threads to use for parallel computation.
        job_id : str, optional
            An optional job ID for tracking the operation.
        write_concurrency : int, optional
            The number of threads to use for writing results.

        Returns
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
        G : GraphV2 | dict[str, Any]
            The graph to estimate for, or a dictionary with nodeCount and relationshipCount.
        prize_property : str
            The name of the node property containing prize values.
        relationship_weight_property : str, optional
            The name of the relationship property to use as weights.
        relationship_types : list[str], optional
            Filter to only use relationships of specific types.
        node_labels : list[str], optional
            Filter to only use nodes with specific labels.
        sudo : bool, default=False
            Whether to run with elevated privileges.
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
