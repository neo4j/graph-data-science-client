from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class K1ColoringEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> K1ColoringMutateResult:
        """
        Runs the K-1 Coloring algorithm and stores the results in the graph catalog as a new node property.

        The K-1 Coloring algorithm assigns a color to every node in the graph, trying to optimize for two objectives:
        to make sure that every neighbor of a given node has a different color than the node itself, and to use as few colors as possible.

        Parameters
        ----------
        G
           Graph object to use
        mutate_property
            Name of the node property to store the results in.
        batch_size
            Number of nodes to process in each batch.
        concurrency
            Number of concurrent threads to use.
        job_id : str | None, default=None
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int, default=10
            Maximum number of iterations to run.
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
        K1ColoringMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> K1ColoringStatsResult:
        """
        Executes the K-1 Coloring algorithm and returns statistics.

        Parameters
        ----------
        G
           Graph object to use
        batch_size
            Number of nodes to process in each batch.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
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
        K1ColoringStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the K-1 Coloring algorithm and returns a stream of results.

        Parameters
        ----------
        G
           Graph object to use
        batch_size
            Number of nodes to process in each batch.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        min_community_size
            Minimum size for communities to be included in results.
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
        batch_size: int = 10000,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> K1ColoringWriteResult:
        """
        Executes the K-1 Coloring algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G
           Graph object to use
        write_property
            Name of the node property to store the results in.
        batch_size
            Number of nodes to process in each batch.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        min_community_size
            Minimum size for communities to be included in results.
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
        K1ColoringWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        batch_size: int = 10000,
        concurrency: int | None = None,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        batch_size
            Number of nodes to process in each batch.
        concurrency
            Number of concurrent threads to use.
        max_iterations
            Maximum number of iterations to run.
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


class K1ColoringMutateResult(BaseResult):
    node_count: int
    color_count: int
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: dict[str, Any]


class K1ColoringStatsResult(BaseResult):
    node_count: int
    color_count: int
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class K1ColoringWriteResult(BaseResult):
    node_count: int
    color_count: int
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
