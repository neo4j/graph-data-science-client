from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        gamma: float | None = 1.0,
        include_intermediate_communities: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int | None = 10,
        node_labels: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        theta: float | None = 0.01,
        tolerance: float | None = 1e-4,
        username: str | None = None,
    ) -> "LeidenMutateResult":
        """
        Executes the Leiden community detection algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs
        gamma : float | None, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool | None, default=False
            Whether to include intermediate communities
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : int | None, default=10
            The maximum number of levels
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The relationship weight property
        seed_property : str | None, default=None
            The seed property
        sudo : bool | None, default=False
            Override memory estimation limits
        theta : float | None, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float | None, default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : str | None, default=None
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        gamma: float | None = 1.0,
        include_intermediate_communities: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int | None = 10,
        node_labels: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        theta: float | None = 0.01,
        tolerance: float | None = 1e-4,
        username: str | None = None,
    ) -> "LeidenStatsResult":
        """
        Executes the Leiden community detection algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs
        gamma : float | None, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool | None, default=False
            Whether to include intermediate communities
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : int | None, default=10
            The maximum number of levels
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The relationship weight property
        seed_property : str | None, default=None
            The seed property
        sudo : bool | None, default=False
            Override memory estimation limits
        theta : float | None, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float | None, default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : str | None, default=None
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        gamma: float | None = 1.0,
        include_intermediate_communities: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int | None = 10,
        min_community_size: int | None = None,
        node_labels: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        theta: float | None = 0.01,
        tolerance: float | None = 1e-4,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Leiden community detection algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs
        gamma : float | None, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool | None, default=False
            Whether to include intermediate communities
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : int | None, default=10
            The maximum number of levels
        min_community_size : int | None, default=None
            The minimum community size
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The relationship weight property
        seed_property : str | None, default=None
            The seed property
        sudo : bool | None, default=False
            Override memory estimation limits
        theta : float | None, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float | None, default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : str | None, default=None
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
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        gamma: float | None = 1.0,
        include_intermediate_communities: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int | None = 10,
        min_community_size: int | None = None,
        node_labels: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        theta: float | None = 0.01,
        tolerance: float | None = 1e-4,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> "LeidenWriteResult":
        """
        Executes the Leiden community detection algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the community ID to
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs
        gamma : float | None, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool | None, default=False
            Whether to include intermediate communities
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_levels : int | None, default=10
            The maximum number of levels
        min_community_size : int | None, default=None
            The minimum community size
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The relationship weight property
        seed_property : str | None, default=None
            The seed property
        sudo : bool | None, default=False
            Override memory estimation limits
        theta : float | None, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float | None, default=1e-4
            The tolerance parameter for the Leiden algorithm
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads for writing

        Returns
        -------
        LeidenWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        consecutive_ids: bool | None = False,
        gamma: float | None = 1.0,
        include_intermediate_communities: bool | None = False,
        max_levels: int | None = 10,
        node_labels: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        theta: float | None = 0.01,
        tolerance: float | None = 1e-4,
    ) -> EstimationResult:
        """
        Estimate the memory requirements for running the Leiden algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to estimate for
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs
        gamma : float | None, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool | None, default=False
            Whether to include intermediate communities
        max_levels : int | None, default=10
            The maximum number of levels
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The relationship weight property
        seed_property : str | None, default=None
            The seed property
        theta : float | None, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float | None, default=1e-4
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
    modularities: list[float]
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
    modularities: list[float]
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
    modularities: list[float]
    modularity: float
    node_count: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_levels: int
    write_millis: int
