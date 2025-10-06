from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LeidenEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> "LeidenMutateResult":
        """
        Executes the Leiden community detection algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        concurrency : Optional[int], default=4
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs
        gamma : Optional[float], default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : Optional[bool], default=False
            Whether to include intermediate communities
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : Optional[int], default=10
            The maximum number of levels
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The relationship weight property
        seed_property : Optional[str], default=None
            The seed property
        sudo : Optional[bool], default=False
            Override memory estimation limits
        theta : Optional[float], default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : Optional[float], default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : Optional[str], default=None
            The username to attribute the procedure run to

        Returns
        -------
        LeidenMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> "LeidenStatsResult":
        """
        Executes the Leiden community detection algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=4
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs
        gamma : Optional[float], default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : Optional[bool], default=False
            Whether to include intermediate communities
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : Optional[int], default=10
            The maximum number of levels
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The relationship weight property
        seed_property : Optional[str], default=None
            The seed property
        sudo : Optional[bool], default=False
            Override memory estimation limits
        theta : Optional[float], default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : Optional[float], default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : Optional[str], default=None
            The username to attribute the procedure run to

        Returns
        -------
        LeidenStatsResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the Leiden community detection algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=4
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs
        gamma : Optional[float], default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : Optional[bool], default=False
            Whether to include intermediate communities
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : Optional[int], default=10
            The maximum number of levels
        min_community_size : Optional[int], default=None
            The minimum community size
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The relationship weight property
        seed_property : Optional[str], default=None
            The seed property
        sudo : Optional[bool], default=False
            Override memory estimation limits
        theta : Optional[float], default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : Optional[float], default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : Optional[str], default=None
            The username to attribute the procedure run to

        Returns
        -------
        DataFrame
            A DataFrame with columns: nodeId, communityId, intermediateCommunityIds
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_levels: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> "LeidenWriteResult":
        """
        Executes the Leiden community detection algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the community ID to
        concurrency : Optional[int], default=4
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs
        gamma : Optional[float], default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : Optional[bool], default=False
            Whether to include intermediate communities
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : Optional[int], default=10
            The maximum number of levels
        min_community_size : Optional[int], default=None
            The minimum community size
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The relationship weight property
        seed_property : Optional[str], default=None
            The seed property
        sudo : Optional[bool], default=False
            Override memory estimation limits
        theta : Optional[float], default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : Optional[float], default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : Optional[str], default=None
            The username to attribute the procedure run to
        write_concurrency : Optional[int], default=None
            The number of concurrent threads for writing
            
        Returns
        -------
        LeidenWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = 4,
        consecutive_ids: Optional[bool] = False,
        gamma: Optional[float] = 1.0,
        include_intermediate_communities: Optional[bool] = False,
        max_levels: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        theta: Optional[float] = 0.01,
        tolerance: Optional[float] = 1e-4,
    ) -> EstimationResult:
        """
        Estimate the memory requirements for running the Leiden algorithm.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to estimate for
        concurrency : Optional[int], default=4
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs
        gamma : Optional[float], default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : Optional[bool], default=False
            Whether to include intermediate communities
        max_levels : Optional[int], default=10
            The maximum number of levels
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The relationship weight property
        seed_property : Optional[str], default=None
            The seed property
        theta : Optional[float], default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : Optional[float], default=1e-4
            The tolerance parameter for the Leiden algorithm

        Returns
        -------
        EstimationResult
            The memory estimation result
        """


class LeidenMutateResult(BaseResult):
    community_count: int
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    modularities: List[float]
    modularity: float
    mutate_millis: int
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_levels: int


class LeidenStatsResult(BaseResult):
    community_count: int
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    modularities: List[float]
    modularity: float
    node_count: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_levels: int


class LeidenWriteResult(BaseResult):
    community_count: int
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    modularities: List[float]
    modularity: float
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_levels: int
    write_millis: int
