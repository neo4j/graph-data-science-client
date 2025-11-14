from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class SccEndpoints(ABC):
    """
    Abstract base class defining the API for the Strongly Connected Components (SCC) algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> SccMutateResult:
        """
        Runs the Strongly Connected Components algorithm and stores the results in the graph catalog as a new node property.

        The Strongly Connected Components (SCC) algorithm finds maximal sets of connected nodes in a directed graph.
        A set is considered a strongly connected component if there is a directed path between each pair of nodes within the set.

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
        job_id : str | None, default=None
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        SccMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> SccStatsResult:
        """
        Executes the SCC algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        SccStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the SCC algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

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
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> SccWriteResult:
        """
        Executes the SCC algorithm and writes the results to the Neo4j database.

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
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        write_concurrency
            Number of concurrent threads to use for writing.Returns
        -------
        SccWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
            The graph to run the algorithm on or a dictionary representing the graph dimensions.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids
            Use consecutive IDs for the components.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class SccMutateResult(BaseResult):
    component_count: int
    component_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class SccStatsResult(BaseResult):
    component_count: int
    component_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class SccWriteResult(BaseResult):
    component_count: int
    component_distribution: dict[str, int | float]
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
