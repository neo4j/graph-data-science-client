from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ModularityOptimizationEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> ModularityOptimizationMutateResult:
        """
        Executes the Modularity Optimization algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=None
            Whether to assign consecutive community IDs
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int
            Maximum number of iterations for the algorithm
        node_labels : list[str]
            Filter nodes with specific labels
        relationship_types : list[str]
            Filter relationships with specific types
        relationship_weight_property : str | None, default=None
            Property name for relationship weights
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo : bool | None, default=False
            Run with elevated privileges
        tolerance : float
            Convergence tolerance for the algorithm
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        ModularityOptimizationMutateResult
            Result containing community statistics and timing information
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> ModularityOptimizationStatsResult:
        """
        Executes the Modularity Optimization algorithm and returns statistics about the communities.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=None
            Whether to assign consecutive community IDs
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int
            Maximum number of iterations for the algorithm
        node_labels : list[str]
            Filter nodes with specific labels
        relationship_types : list[str]
            Filter relationships with specific types
        relationship_weight_property : str | None, default=None
            Property name for relationship weights
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo : bool | None, default=False
            Run with elevated privileges
        tolerance : float
            Convergence tolerance for the algorithm
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        ModularityOptimizationStatsResult
            Result containing community statistics and timing information
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Modularity Optimization algorithm and returns the results as a DataFrame.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=None
            Whether to assign consecutive community IDs
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int
            Maximum number of iterations for the algorithm
        min_community_size : int | None, default=None
            Minimum size for communities to be included in results
        node_labels : list[str]
            Filter nodes with specific labels
        relationship_types : list[str]
            Filter relationships with specific types
        relationship_weight_property : str | None, default=None
            Property name for relationship weights
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo : bool | None, default=False
            Run with elevated privileges
        tolerance : float
            Convergence tolerance for the algorithm
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        DataFrame
            A DataFrame with columns 'nodeId' and 'communityId'
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        tolerance: float = 0.0001,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> ModularityOptimizationWriteResult:
        """
        Executes the Modularity Optimization algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the community ID for each node
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=None
            Whether to assign consecutive community IDs
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int
            Maximum number of iterations for the algorithm
        min_community_size : int | None, default=None
            Minimum size for communities to be included in results
        node_labels : list[str]
            Filter nodes with specific labels
        relationship_types : list[str]
            Filter relationships with specific types
        relationship_weight_property : str | None, default=None
            Property name for relationship weights
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo : bool | None, default=False
            Run with elevated privileges
        tolerance : float
            Convergence tolerance for the algorithm
        username : str | None, default=None
            Username for authentication
        write_concurrency : int | None, default=None
            The number of concurrent threads for writing

        Returns
        -------
        ModularityOptimizationWriteResult
            Result containing community statistics and timing information
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        tolerance: float = 0.0001,
    ) -> EstimationResult:
        """
        Estimates the memory consumption for running the Modularity Optimization algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph configuration or graph object
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=None
            Whether to assign consecutive community IDs
        max_iterations : int
            Maximum number of iterations for the algorithm
        node_labels : list[str]
            Filter nodes with specific labels
        relationship_types : list[str]
            Filter relationships with specific types
        relationship_weight_property : str | None, default=None
            Property name for relationship weights
        seed_property : str | None, default=None
            Property name for initial community assignments
        tolerance : float
            Convergence tolerance for the algorithm

        Returns
        -------
        EstimationResult
            Estimated memory consumption and other metrics
        """
        pass


class ModularityOptimizationMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    nodes: int
    did_converge: bool
    ran_iterations: int
    modularity: float
    community_count: int
    community_distribution: dict[str, float]
    configuration: dict[str, Any]


class ModularityOptimizationStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    nodes: int
    did_converge: bool
    ran_iterations: int
    modularity: float
    community_count: int
    community_distribution: dict[str, float]
    configuration: dict[str, Any]


class ModularityOptimizationWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    nodes: int
    did_converge: bool
    ran_iterations: int
    modularity: float
    community_count: int
    community_distribution: dict[str, float]
    configuration: dict[str, Any]
