from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class WccEndpoints(ABC):
    """
    Abstract base class defining the API for the Weakly Connected Components (WCC) algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        threshold: float = 0.0,
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
    ) -> WccMutateResult:
        """
        Runs the Weakly Connected Components algorithm and stores the results in the graph catalog as a new node property.

        The Weakly Connected Components (WCC) algorithm finds sets of connected nodes in directed and undirected graphs where two nodes are connected if there exists a path between them.
        In contrast to Strongly Connected Components (SCC), the direction of relationships on the path between two nodes is not considered.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        threshold : float, default=0.0
            The minimum required weight to consider a relationship during traversal
        relationship_types : list[str]
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
        WccMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        threshold: float = 0.0,
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
    ) -> WccStatsResult:
        """
        Executes the WCC algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        threshold : float, default=0.0
            The minimum required weight to consider a relationship during traversal
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
            Defines node properties that are used as initial component identifiers
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        WccStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        min_component_size: int | None = None,
        threshold: float = 0.0,
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
    ) -> DataFrame:
        """
        Executes the WCC algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        min_component_size : int | None, default=None
            Don't stream components with fewer nodes than this
        threshold : float, default=0.0
            The minimum required weight to consider a relationship during traversal
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
            Defines node properties that are used as initial component identifiers
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

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
        min_component_size: int | None = None,
        threshold: float = 0.0,
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
    ) -> WccWriteResult:
        """
        Executes the WCC algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        min_component_size : int | None, default=None
            Don't write components with fewer nodes than this
        threshold : float, default=0.0
            The minimum required weight to consider a relationship during traversal
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
            Defines node properties that are used as initial component identifiers
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.
        write_concurrency
            Number of concurrent threads to use for writing.Returns
        -------
        WccWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        threshold: float = 0.0,
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
        threshold : float, default=0.0
            The minimum required weight to consider a relationship during traversal
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        seed_property : str | None, default=None
            A property to use as the starting component id for a node
        consecutive_ids
            Use consecutive IDs for the components.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class WccMutateResult(BaseResult):
    component_count: int
    component_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class WccStatsResult(BaseResult):
    component_count: int
    component_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class WccWriteResult(BaseResult):
    component_count: int
    component_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
