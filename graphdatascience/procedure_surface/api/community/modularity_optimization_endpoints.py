from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ModularityOptimizationEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
    ) -> ModularityOptimizationMutateResult:
        """
        Executes the Modularity Optimization algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        batch_size : Optional[int], default=None
            Number of nodes to process in each batch
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=None
            Whether to assign consecutive community IDs
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=None
            Maximum number of iterations for the algorithm
        node_labels : Optional[List[str]], default=None
            Filter nodes with specific labels
        relationship_types : Optional[List[str]], default=None
            Filter relationships with specific types
        relationship_weight_property : Optional[str], default=None
            Property name for relationship weights
        seed_property : Optional[str], default=None
            Property name for initial community assignments
        sudo : Optional[bool], default=False
            Run with elevated privileges
        tolerance : Optional[float], default=None
            Convergence tolerance for the algorithm
        username : Optional[str], default=None
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
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
    ) -> ModularityOptimizationStatsResult:
        """
        Executes the Modularity Optimization algorithm and returns statistics about the communities.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        batch_size : Optional[int], default=None
            Number of nodes to process in each batch
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=None
            Whether to assign consecutive community IDs
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=None
            Maximum number of iterations for the algorithm
        node_labels : Optional[List[str]], default=None
            Filter nodes with specific labels
        relationship_types : Optional[List[str]], default=None
            Filter relationships with specific types
        relationship_weight_property : Optional[str], default=None
            Property name for relationship weights
        seed_property : Optional[str], default=None
            Property name for initial community assignments
        sudo : Optional[bool], default=False
            Run with elevated privileges
        tolerance : Optional[float], default=None
            Convergence tolerance for the algorithm
        username : Optional[str], default=None
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
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the Modularity Optimization algorithm and returns the results as a DataFrame.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        batch_size : Optional[int], default=None
            Number of nodes to process in each batch
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=None
            Whether to assign consecutive community IDs
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=None
            Maximum number of iterations for the algorithm
        min_community_size : Optional[int], default=None
            Minimum size for communities to be included in results
        node_labels : Optional[List[str]], default=None
            Filter nodes with specific labels
        relationship_types : Optional[List[str]], default=None
            Filter relationships with specific types
        relationship_weight_property : Optional[str], default=None
            Property name for relationship weights
        seed_property : Optional[str], default=None
            Property name for initial community assignments
        sudo : Optional[bool], default=False
            Run with elevated privileges
        tolerance : Optional[float], default=None
            Convergence tolerance for the algorithm
        username : Optional[str], default=None
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
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = None,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        tolerance: Optional[float] = None,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> ModularityOptimizationWriteResult:
        """
        Executes the Modularity Optimization algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the community ID for each node
        batch_size : Optional[int], default=None
            Number of nodes to process in each batch
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=None
            Whether to assign consecutive community IDs
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=None
            Maximum number of iterations for the algorithm
        min_community_size : Optional[int], default=None
            Minimum size for communities to be included in results
        node_labels : Optional[List[str]], default=None
            Filter nodes with specific labels
        relationship_types : Optional[List[str]], default=None
            Filter relationships with specific types
        relationship_weight_property : Optional[str], default=None
            Property name for relationship weights
        seed_property : Optional[str], default=None
            Property name for initial community assignments
        sudo : Optional[bool], default=False
            Run with elevated privileges
        tolerance : Optional[float], default=None
            Convergence tolerance for the algorithm
        username : Optional[str], default=None
            Username for authentication
        write_concurrency : Optional[int], default=None
            The number of concurrent threads for writing
        write_to_result_store : Optional[bool], default=None
            Whether to write results to the result store

        Returns
        -------
        ModularityOptimizationWriteResult
            Result containing community statistics and timing information
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        tolerance: Optional[float] = None,
    ) -> EstimationResult:
        """
        Estimates the memory consumption for running the Modularity Optimization algorithm.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph configuration or graph object
        batch_size : Optional[int], default=None
            Number of nodes to process in each batch
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=None
            Whether to assign consecutive community IDs
        max_iterations : Optional[int], default=None
            Maximum number of iterations for the algorithm
        node_labels : Optional[List[str]], default=None
            Filter nodes with specific labels
        relationship_types : Optional[List[str]], default=None
            Filter relationships with specific types
        relationship_weight_property : Optional[str], default=None
            Property name for relationship weights
        seed_property : Optional[str], default=None
            Property name for initial community assignments
        tolerance : Optional[float], default=None
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
    community_distribution: Dict[str, float]
    configuration: Dict[str, Any]


class ModularityOptimizationStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    nodes: int
    did_converge: bool
    ran_iterations: int
    modularity: float
    community_count: int
    community_distribution: Dict[str, float]
    configuration: Dict[str, Any]


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
    community_distribution: Dict[str, float]
    configuration: Dict[str, Any]
