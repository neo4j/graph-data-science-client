from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class K1ColoringEndpoints(ABC):
    """
    Abstract base class defining the API for the K-1 Coloring algorithm.
    """

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
        Executes the K-1 Coloring algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the color for each node
        batch_size : int, default=10000
            The batch size for processing
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int, default=10
            The maximum number of iterations of K-1 Coloring to run
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
        G : GraphV2
            The graph to run the algorithm on
        batch_size : int, default=10000
            The batch size for processing
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int, default=10
            The maximum number of iterations of K-1 Coloring to run
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
        G : GraphV2
            The graph to run the algorithm on
        batch_size : int, default=10000
            The batch size for processing
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int, default=10
            The maximum number of iterations of K-1 Coloring to run
        min_community_size : int | None, default=None
            Only community ids of communities with a size greater than or equal to the given value are returned
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types considered in this algorithm run
        sudo : bool, default=False
            Override memory estimation limits
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
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write colors to
        batch_size : int, default=10000
            The batch size for processing
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int, default=10
            The maximum number of iterations of K-1 Coloring to run
        min_community_size : int | None, default=None
            Only community ids of communities with a size greater than or equal to the given value are written to Neo4j
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
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        batch_size : int, default=10000
            The batch size for processing
        concurrency : int | None, default=None
            The number of concurrent threads
        max_iterations : int, default=10
            The maximum number of iterations of K-1 Coloring to run
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
