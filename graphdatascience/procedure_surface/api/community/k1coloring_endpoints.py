from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
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
        batch_size: int | None = None,
        max_iterations: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> K1ColoringMutateResult:
        """
        Executes the K-1 Coloring algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the color for each node
        batch_size : int | None, default=None
            The batch size for processing
        max_iterations : int | None, default=None
            The maximum number of iterations of K-1 Coloring to run
        relationship_types : list[str] | None, default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job

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
        batch_size: int | None = None,
        max_iterations: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> K1ColoringStatsResult:
        """
        Executes the K-1 Coloring algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        batch_size : int | None, default=None
            The batch size for processing
        max_iterations : int | None, default=None
            The maximum number of iterations of K-1 Coloring to run
        relationship_types : list[str] | None, default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job

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
        batch_size: int | None = None,
        max_iterations: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        min_community_size: int | None = None,
    ) -> DataFrame:
        """
        Executes the K-1 Coloring algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        batch_size : int | None, default=None
            The batch size for processing
        max_iterations : int | None, default=None
            The maximum number of iterations of K-1 Coloring to run
        relationship_types : list[str] | None, default=None
            The relationships types considered in this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        min_community_size : int | None, default=None
            Only community ids of communities with a size greater than or equal to the given value are returned

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
        batch_size: int | None = None,
        max_iterations: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: Any | None = None,
        min_community_size: int | None = None,
    ) -> K1ColoringWriteResult:
        """
        Executes the K-1 Coloring algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write colors to
        batch_size : int | None, default=None
            The batch size for processing
        max_iterations : int | None, default=None
            The maximum number of iterations of K-1 Coloring to run
        relationship_types : list[str] | None, default=None
            The relationships types considered in this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        write_concurrency : Any | None, default=None
            The number of concurrent threads during the write phase
        min_community_size : int | None, default=None
            Only community ids of communities with a size greater than or equal to the given value are written to Neo4j

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
        batch_size: int | None = None,
        max_iterations: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        batch_size : int | None, default=None
            The batch size for processing
        max_iterations : int | None, default=None
            The maximum number of iterations of K-1 Coloring to run
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads

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
