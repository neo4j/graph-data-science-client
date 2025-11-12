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
        mutate_property : str
            Name of the node property to store the results in.
        concurrency : int | None, default=None
            Number of CPU threads to use.
        consecutive_ids : bool, default=False
            Whether to use consecutive IDs for components
        job_id : str | None, default=None
            Identifier for the computation.
        log_progress : bool, default=True
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
        username : str | None, default=None
            As an administrator, run the algorithm as a different user, to access also their graphs.

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
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool, default=False
            Flag to decide whether component identifiers are mapped into a consecutive id space
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

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
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool, default=False
            Flag to decide whether component identifiers are mapped into a consecutive id space
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types : list[str]
            The relationship types considered in this algorithm run
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

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
        write_property : str
            The property name to write component IDs to
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool, default=False
            Flag to decide whether component identifiers are mapped into a consecutive id space
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types : list[str]
            The relationship types considered in this algorithm run
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads during the write phase

        Returns
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
            The graph to run the algorithm on or a dictionary representing the graph.
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool, default=False
            Flag to decide if the component identifiers should be returned consecutively or not
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run

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
