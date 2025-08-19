from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult

from ...graph.graph_object import Graph
from .estimation_result import EstimationResult


class LouvainEndpoints(ABC):
    """
    Abstract base class defining the API for the Louvain algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> LouvainMutateResult:
        """
        Executes the Louvain algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        tolerance : Optional[float], default=None
            The tolerance value for the algorithm convergence
        max_levels : Optional[int], default=None
            The maximum number of levels in the hierarchy
        include_intermediate_communities : Optional[bool], default=None
            Whether to include intermediate community assignments
        max_iterations : Optional[int], default=None
            The maximum number of iterations per level
        relationship_types : Optional[List[str]], default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether community identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        LouvainMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> LouvainStatsResult:
        """
        Executes the Louvain algorithm and returns statistics.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        tolerance : Optional[float], default=None
            The tolerance value for the algorithm convergence
        max_levels : Optional[int], default=None
            The maximum number of levels in the hierarchy
        include_intermediate_communities : Optional[bool], default=None
            Whether to include intermediate community assignments
        max_iterations : Optional[int], default=None
            The maximum number of iterations per level
        relationship_types : Optional[List[str]], default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether community identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        LouvainStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        min_community_size: Optional[int] = None,
    ) -> DataFrame:
        """
        Executes the Louvain algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        tolerance : Optional[float], default=None
            The tolerance value for the algorithm convergence
        max_levels : Optional[int], default=None
            The maximum number of levels in the hierarchy
        include_intermediate_communities : Optional[bool], default=None
            Whether to include intermediate community assignments
        max_iterations : Optional[int], default=None
            The maximum number of iterations per level
        relationship_types : Optional[List[str]], default=None
            The relationships types considered in this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether community identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        min_community_size : Optional[int], default=None
            Don't stream communities with fewer nodes than this

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        write_property: str,
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
        min_community_size: Optional[int] = None,
    ) -> LouvainWriteResult:
        """
        Executes the Louvain algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write community IDs to
        tolerance : Optional[float], default=None
            The tolerance value for the algorithm convergence
        max_levels : Optional[int], default=None
            The maximum number of levels in the hierarchy
        include_intermediate_communities : Optional[bool], default=None
            Whether to include intermediate community assignments
        max_iterations : Optional[int], default=None
            The maximum number of iterations per level
        relationship_types : Optional[List[str]], default=None
            The relationships types considered in this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether community identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase
        min_community_size : Optional[int], default=None
            Don't write communities with fewer nodes than this

        Returns
        -------
        LouvainWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        tolerance: Optional[float] = None,
        max_levels: Optional[int] = None,
        include_intermediate_communities: Optional[bool] = None,
        max_iterations: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        tolerance : Optional[float], default=None
            The tolerance value for the algorithm convergence
        max_levels : Optional[int], default=None
            The maximum number of levels in the hierarchy
        include_intermediate_communities : Optional[bool], default=None
            Whether to include intermediate community assignments
        max_iterations : Optional[int], default=None
            The maximum number of iterations per level
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        seed_property : Optional[str], default=None
            A property to use as the starting community id for a node
        consecutive_ids : Optional[bool], default=None
            Flag to decide if the component identifiers should be returned consecutively or not
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class LouvainMutateResult(BaseResult):
    modularity: float
    modularities: List[Any]
    ran_levels: int
    community_count: int
    community_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class LouvainStatsResult(BaseResult):
    modularity: float
    modularities: List[Any]
    ran_levels: int
    community_count: int
    community_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class LouvainWriteResult(BaseResult):
    modularity: float
    modularities: List[Any]
    ran_levels: int
    community_count: int
    community_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
