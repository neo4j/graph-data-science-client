from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class KCoreEndpoints(ABC):
    """
    Abstract base class defining the API for the K-Core decomposition algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> KCoreMutateResult:
        """
        Runs the K-Core Decomposition algorithm and stores the results in the graph catalog as a new node property.

        The K-core decomposition constitutes a process that separates the nodes in a graph into groups based on the degree sequence and topology of the graph.
        The term `i-core` refers to a maximal subgraph of the original graph such that each node in this subgraph has degree at least `i`.
        Each node is associated with a core value which denotes the largest value `i` such that the node belongs to the `i-core`.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            Name of the node property to store the results in.
        concurrency : int | None, default=None
            Number of CPU threads to use.
        job_id : str | None, default=None
            Identifier for the computation.
        log_progress : bool, default=True
            Display progress logging.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo : bool, default=False
            Disable the memory guard.
        username : str | None, default=None
            As an administrator, run the algorithm as a different user, to access also their graphs.

        Returns
        -------
        KCoreMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> KCoreStatsResult:
        """
        Executes the K-Core algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        sudo : bool, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        KCoreStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the K-Core algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        relationship_types : list[str]
            The relationships types considered in this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        sudo : bool
            Override memory estimation limits
        log_progress : bool, default=True
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads
        job_id : str | None
            An identifier for the job

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing nodeId and coreValue
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> KCoreWriteResult:
        """
        Executes the K-Core algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The property name to write core values to
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types considered in this algorithm run
        sudo : bool, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        KCoreWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
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
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class KCoreMutateResult(BaseResult):
    node_properties_written: int
    degeneracy: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    configuration: dict[str, Any]


class KCoreStatsResult(BaseResult):
    degeneracy: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class KCoreWriteResult(BaseResult):
    node_properties_written: int
    degeneracy: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    configuration: dict[str, Any]
