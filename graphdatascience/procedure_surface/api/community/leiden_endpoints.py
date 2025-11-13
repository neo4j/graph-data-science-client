from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LeidenEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> LeidenMutateResult:
        """
        Executes the Leiden community detection algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        gamma : float, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool, default=False
            Whether to include intermediate communities
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_levels : int, default=10
            The maximum number of levels
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The seed property
        sudo
            Disable the memory guard.
        theta : float, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float, default=0.0001
            The tolerance parameter for the Leiden algorithm
        username
            As an administrator, impersonate a different user for accessing their graphs.

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
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> LeidenStatsResult:
        """
        Executes the Leiden community detection algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        gamma : float, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool, default=False
            Whether to include intermediate communities
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_levels : int, default=10
            The maximum number of levels
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The seed property
        sudo
            Disable the memory guard.
        theta : float, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float, default=0.0001
            The tolerance parameter for the Leiden algorithm
        username
            As an administrator, impersonate a different user for accessing their graphs.

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
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Leiden community detection algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        gamma : float, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool, default=False
            Whether to include intermediate communities
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_levels : int, default=10
            The maximum number of levels
        min_community_size : int | None, default=None
            The minimum community size
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The seed property
        sudo
            Disable the memory guard.
        theta : float, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float, default=0.0001
            The tolerance parameter for the Leiden algorithm
        username
            As an administrator, impersonate a different user for accessing their graphs.

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
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_levels: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        theta: float = 0.01,
        tolerance: float = 0.0001,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> LeidenWriteResult:
        """
        Executes the Leiden community detection algorithm and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        gamma : float, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool, default=False
            Whether to include intermediate communities
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_levels : int, default=10
            The maximum number of levels
        min_community_size : int | None, default=None
            The minimum community size
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The seed property
        sudo
            Disable the memory guard.
        theta : float, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float, default=0.0001
            The tolerance parameter for the Leiden algorithm
        username
            As an administrator, impersonate a different user for accessing their graphs.
        write_concurrency
            Number of concurrent threads to use for writing.Returns
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
        consecutive_ids: bool = False,
        gamma: float = 1.0,
        include_intermediate_communities: bool = False,
        max_levels: int = 10,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        theta: float = 0.01,
        tolerance: float = 0.0001,
    ) -> EstimationResult:
        """
        Estimate the memory requirements for running the Leiden algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to estimate for
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        gamma : float, default=1.0
            The gamma parameter for the Leiden algorithm
        include_intermediate_communities : bool, default=False
            Whether to include intermediate communities
        max_levels : int, default=10
            The maximum number of levels
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The seed property
        theta : float, default=0.01
            The theta parameter for the Leiden algorithm
        tolerance : float, default=0.0001
            The tolerance parameter for the Leiden algorithm

        Returns
        -------
        EstimationResult
            The memory estimation result
        """


class LeidenMutateResult(BaseResult):
    community_count: int
    community_distribution: dict[str, int | float]
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
    community_distribution: dict[str, int | float]
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
    community_distribution: dict[str, int | float]
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
