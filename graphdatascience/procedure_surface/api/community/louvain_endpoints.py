from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LouvainEndpoints(ABC):
    """
    Abstract base class defining the API for the Louvain algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        tolerance: float = 0.0001,
        max_levels: int = 10,
        include_intermediate_communities: bool = False,
        max_iterations: int = 10,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> LouvainMutateResult:
        """
        Runs the Louvain algorithm and stores the results in the graph catalog as a new node property.

        The Louvain method is an algorithm to detect communities in large networks.
        It maximizes a modularity score for each community, where the modularity quantifies the quality of an assignment of nodes to communities by evaluating how much more densely connected the nodes within a community are, compared to how connected they would be in a random network.
        The Louvain algorithm is a hierarchical clustering algorithm that recursively merges communities into a single node and runs the modularity clustering on the condensed graphs.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        tolerance
            Minimum change in scores between iterations.
        max_levels : int, default=10
            The maximum number of levels in the hierarchy
        include_intermediate_communities : bool, default=False
            Whether to include intermediate communities
        max_iterations
            Maximum number of iterations to run per level.
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
        job_id : str | None, default=None
            Identifier for the computation.
        seed_property : str | None, default=None
            The property name that contains seed values
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        LouvainMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        tolerance: float = 0.0001,
        max_levels: int = 10,
        include_intermediate_communities: bool = False,
        max_iterations: int = 10,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> LouvainStatsResult:
        """
        Executes the Louvain algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        tolerance
            Minimum change in scores between iterations.
        max_levels : int, default=10
            The maximum number of levels in the hierarchy
        include_intermediate_communities : bool, default=False
            Whether to include intermediate community assignments
        max_iterations
            Maximum number of iterations to run per level.
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        LouvainStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        tolerance: float = 0.0001,
        max_levels: int = 10,
        include_intermediate_communities: bool = False,
        max_iterations: int = 10,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
        min_community_size: int | None = None,
    ) -> DataFrame:
        """
        Executes the Louvain algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        tolerance
            Minimum change in scores between iterations.
        max_levels : int, default=10
            The maximum number of levels in the hierarchy
        include_intermediate_communities : bool, default=False
            Whether to include intermediate community assignments
        max_iterations
            Maximum number of iterations to run per level.
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.
        min_community_size : int | None, default=None
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
        G: GraphV2,
        write_property: str,
        tolerance: float = 0.0001,
        max_levels: int = 10,
        include_intermediate_communities: bool = False,
        max_iterations: int = 10,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
        write_concurrency: int | None = None,
        min_community_size: int | None = None,
    ) -> LouvainWriteResult:
        """
        Executes the Louvain algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        tolerance
            Minimum change in scores between iterations.
        max_levels : int, default=10
            The maximum number of levels in the hierarchy
        include_intermediate_communities : bool, default=False
            Whether to include intermediate community assignments
        max_iterations
            Maximum number of iterations to run per level.
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial community identifiers
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.
        write_concurrency
            Number of concurrent threads to use for writing.
        min_community_size : int | None, default=None
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
        G: GraphV2 | dict[str, Any],
        tolerance: float = 0.0001,
        max_levels: int = 10,
        include_intermediate_communities: bool = False,
        max_iterations: int = 10,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool = False,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
            The graph to run the algorithm on or a dictionary representing the graph dimensions.
        tolerance
            Minimum change in scores between iterations.
        max_levels : int, default=10
            The maximum number of levels in the hierarchy
        include_intermediate_communities : bool, default=False
            Whether to include intermediate community assignments
        max_iterations
            Maximum number of iterations to run per level.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        seed_property : str | None, default=None
            A property to use as the starting community id for a node
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class LouvainMutateResult(BaseResult):
    modularity: float
    modularities: list[float]
    ran_levels: int
    community_count: int
    community_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class LouvainStatsResult(BaseResult):
    modularity: float
    modularities: list[float]
    ran_levels: int
    community_count: int
    community_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class LouvainWriteResult(BaseResult):
    modularity: float
    modularities: list[float]
    ran_levels: int
    community_count: int
    community_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
