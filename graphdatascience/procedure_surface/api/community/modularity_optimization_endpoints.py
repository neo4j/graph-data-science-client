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
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int
            Maximum number of iterations for the algorithm
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo
            Disable the memory guard.
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
        G
            The graph to run the algorithm on.
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int
            Maximum number of iterations for the algorithm
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo
            Disable the memory guard.
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
        G
            The graph to run the algorithm on.
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int
            Maximum number of iterations for the algorithm
        min_community_size : int | None, default=None
            Minimum size for communities to be included in results
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo
            Disable the memory guard.
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
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        batch_size : int | None, default=None
            Number of nodes to process in each batch
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int
            Maximum number of iterations for the algorithm
        min_community_size : int | None, default=None
            Minimum size for communities to be included in results
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            Property name for initial community assignments
        sudo
            Disable the memory guard.
        tolerance : float
            Convergence tolerance for the algorithm
        username : str | None, default=None
            Username for authentication
        write_concurrency
            Number of concurrent threads to use for writing.Returns
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
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        max_iterations : int
            Maximum number of iterations for the algorithm
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
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
